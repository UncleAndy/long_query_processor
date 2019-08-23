#!/usr/bin/perl

use strict;
use warnings;

use File::Basename;
use YAML;
use DBI;
use POSIX;
use JSON;
use List::MoreUtils qw(uniq);

# Пул работающих форков
my $forks = {};

# Отслеживание завершения дочерних процессов
$SIG{CHLD} = sub {
    while () {
        my $child = waitpid -1, POSIX::WNOHANG;
        last if $child <= 0;
        delete $forks->{ $child };
        print "Working process $child finished\n";
    }
};

# Считываем конфиг и определеяем настройки подключения к БД
my $dirname = dirname(__FILE__);
my $config = $dirname.'/config.yaml'
if (! -e $config) {
    die "Config file '$config' not exists!";
}

my $cfg = YAML::Load($config);

sub open_db {
    my ($db_cfg, $options) = @_;
    $options //= {AutoCommit => 1, RaiseError => 1, PrintError => 1};

    return DBI->connect_cached(
        "dbi:Pg:host=".$db_cfg->{ host }.";port=".$db_cfg->{ port }.";dbname=".$db_cfg->{ name },
        $db_cfg->{ user },
        $db_cfg->{ pass },
        $options )
        or die $DBI::errstr;
};

# Рабочий цикл
while (1) {
    # Если количество форков равно максимальному - пауза и в начало цикла
    if ( scalar keys %{$forks} >= $cfg->{ processor }->{ max_forks } ) {
        sleep $cfg->{ processor }->{ sleep_max_forks };
        next;
    }

    # Берем первый необработанный таск
    # Используем FOR UPDATE что-бы можно было запускать несколько экземпляров
    # процессоров (для горизонтального масштабирования)
    my $queue_dbh = open_db( $cfg->{ db }->{ queue }, {AutoCommit => 0, RaiseError => 1, PrintError => 1} );
    my $c = $queue_dbh->prepare('SELECT id, query FROM query_queue WHERE status = 0 ORDER BY id LIMIT 1 FOR UPDATE');
    $c->execute();
    my ($query_id, $query) = $c->fetchrow_array();
    $c->finish;

    # Если нет необработанных - пауза и с начала цикла
    if ( !defined($query_id) ) {
        $queue_dbh->commit;
        sleep $cfg->{ processor }->{ sleep_no_queries };
        next;
    }

    $queue_dbh->do('UPDATE query_queue SET status = 1 WHERE id = ?', undef, $query_id);
    $queue_dbh->commit;

    # Запускаем fork, передаем в него query_id и query и регистрируем его в $forks
    if ( my $child_pid = fork() ) {
        # Главный процесс
        $forks->{ $child_pid } = { query_id => $query_id };
    } else {
        print "Working process $$ started...\n";
        process_query( $queue_dbh, $query_id, $query );

        # Завершаем дочерний процесс
        print "Working process $$ process finished\n";
        exit;
    }
}

sub process_query {
    my ( $queue_dbh, $query_id, $query ) = @_;

    # Общий порядок работы:
    # 1. Проверяем есть-ли в условиях запроса фильтрация по имени организации-владельца или организации-контрагента
    # 1.1. Если есть - делаем выборку по ним и формируем в БД с таблицей invoices одну или две временные таблицы с этими данными
    # 2. Формируем SQL запрос по таблице invoices на основе указанных фильтров и имеющихся временных таблиц
    # 3. Если в п.1. отсутствует одна из таблиц организации, формируем запрос на organizations для заполнения названий организаций
    my $params = {};
    eval {
        $params = decode_json($query);
    };
    if ( defined($@) ) {
        $queue_dbh->do('UPDATE query_queue SET status = 3, notification = ? WHERE id = ?', undef,
            "ERROR: Can not parse JSON query: $@", $query_id);
        $queue_dbh->commit;
        return;
    }

    my $inv_dbh = open_db( $cfg->{ db }->{ invoices }, {AutoCommit => 0, RaiseError => 1, PrintError => 1} );

    # Если нужно - готовим коннект к БД организаций
    my $org_dbh = open_db( $cfg->{ db }->{ organizations }, {AutoCommit => 1, RaiseError => 1, PrintError => 1} )
        if $params->{ owner_name } || $params->{ contractor_name };

    my $owner_name_table = '';
    if ( $params->{ owner_name } ) {
        $owner_name_table = 'owner_names';
        create_name_table( $org_dbh, $inv_dbh, $owner_name_table, $params->{ owner_name } );
    }

    my $contr_name_table = '';
    if ( $params->{ contractor_name } ) {
        # Создаем временную таблицу в БД счетов-фактур
        $contr_name_table = 'contractor_names';
        create_name_table( $org_dbh, $inv_dbh, $contr_name_table, $params->{ contractor_name } );
    }

    # Формируем запрос для счетов-фактур
    my $owner_name_field_sql = '';
    my $owner_name_table_sql = '';
    my $owner_name_where_sql = '';
    if ( $owner_name_table ne '' ) {
        $owner_name_field_sql = ", on.name as owner_name ";
        $owner_name_table_sql = ', '.$owner_name_table.' on ';
        $owner_name_where_sql = ' AND  i.inn = on.inn ';
    }

    my $contr_name_field_sql = '';
    my $contr_name_table_sql = '';
    my $contr_name_where_sql = '';
    if ( $contr_name_table ne '' ) {
        $contr_name_field_sql = ", cn.name as contractor_name ";
        $contr_name_table_sql = ', '.$contr_name_table.' cn ';
        $contr_name_where_sql = ' AND  i.inn = cn.inn ';
    }


    # Выборка по полям счетов фактур
    my ( $main_where_sql, $params_list ) = prepare_invoice_params($params, ['period', 'owner_inn', 'type', 'contractor_inn', 'date', 'number']);

    my $sql = '
        SELECT
            i.period, i.owner_inn, i.type, i.contractor_inn, i.date, i.number'
            .$owner_name_field_sql.$contr_name_field_sql.'
        FROM invoices i'.$owner_name_table_sql.$contr_name_table_sql.'
        WHERE '.$main_where_sql.$owner_name_where_sql.$contr_name_where_sql;
    my $c = $inv_dbh->prepare($sql);
    $c->execute(@{$params_list});
    while ( my $row = $c->fetchrow_hashref() ) {
        $queue_dbh->do("INSERT INTO query_results
            (query_id, period, owner_inn, type, contractor_inn, date, number, owner_name, contractor_name)
            VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?)", undef,
            $query_id, $row->{ period }, $row->{ owner_inn }, $row->{ type }, $row->{ contractor_inn }, $row->{ date },
            $row->{ number }, $row->{ owner_name }, $row->{ contractor_name });
    }
    $c->finish;

    # Если не было поиска по какому-либо из имен - заполняем отдельно
    fill_names($queue_dbh, $org_dbh, $query_id, $owner_name_table, $contr_name_table);

    # Отмечаем запрос как исполненный
    $queue_dbh->do('UPDATE query_queue SET status = 2 WHERE id = ?', undef, $query_id);
    $queue_dbh->commit;
}

sub create_name_table {
    my ($org_dbh, $inv_dbh, $table_name, $find_str) = @_;

    # Создаем временную таблицу в БД счетов-фактур
    $inv_dbh->do('CREATE TEMP TABLE '.$table_name.' (inn varchar NOT NULL PRIMARY KEY, name varchar)');

    # Выборка по имени организации
    my $c = $org_dbh->prepare('SELECT inn, name FROM organizations WHERE name LIKE ?');
    $c->execute( $find_str );
    while ( my ($inn, $name) = fetchrow_array() ) {
        $inv_dbh->do('INSERT INTO '.$table_name.' (inn, name) VALUES (?, ?)', undef,
            $inn, $name);
    };
    $c->finish;
}

sub prepare_invoice_params {
    my ($params, $list) = @_;

    my $main_where_sql = '';
    my $params_list = ();

    foreach my $field (@{list}) {
        if ( defined( $params->{ $field } ) ) {
            $main_where_sql .= ' i.'.$field.' = ? ';
            push @{$params_list}, $params->{ field };
        }
    }

    $main_where_sql = " 't' " if $main_where_sql eq '';

    return $main_where_sql, $params_list;
}

sub fill_names {
    my ($queue_dbh, $org_dbh, $query_id, $owner_name_table, $contr_name_table) = @_;

    my $batch_size = 1000;
    my $offset = 0;
    while (1) {
        my $owner_recs = {};  # соответствие inn id записи (inn - ключ)
        my $contr_recs = {};  # соответствие inn id записи (inn - ключ)

        my $count = 0;
        my $c = $queue_dbh->prepare('SELECT id, owner_inn, contractor_inn
            FROM query_results
            WHERE query_id = ?
            ORDER BY id
            OFFSET ?
            LIMIT ?');
        $c->execute($query_id, $offset, $batch_size);
        while ( my ($id, $owner_inn, $contr_inn) = $c->fetchrow_array() ) {
            $count++;

            $owner_recs->{$owner_inn} = $id if $owner_name_table eq '';
            $contr_recs->{$contr_inn} = $id if $contr_name_table eq '';
        };
        $c->finish;

        # Сдвигаем смещение для следующей выборки
        $offset += $count;

        # Формируем общий список распознаваемых ИНН
        my @_inns = keys %{$owner_recs};
        push @_inns, keys %{$contr_recs};
        my @inns = uniq(@_inns);

        # Выбираем все ИНН по списку и обновляем имена в релультатах
        my $params_list = ("?," x (length(@inns) - 1))."?";
        $c = $org_dbh->prepare('SELECT inn, name FROM organizations WHERE inn IN ('.$params_list.')');
        $c->execute(@inns);
        while ( my ($inn, $name) = $c->fetchrow_array() ) {
            $queue_dbh->do('UPDATE query_results SET owner_name = ? WHERE id = ?', undef,
                $name, $owner_recs->{$inn}) if defined( $owner_recs->{ $inn } );
            $queue_dbh->do('UPDATE query_results SET contractor_name = ? WHERE id = ?', undef,
                $name, $contr_recs->{$inn}) if defined( $contr_recs->{ $inn } );
        }
        $c->finish;

        # Если больше нет записей в выборке, значит все обработали
        last if $count = 0;
    }
}
