#!/usr/bin/perl

use strict;

use Plack;
use Plack::Request;
use Plack::Builder;
use File::Basename;
use YAML;
use File::Temp;

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

my $query = sub {
    my $env = shift;
    my $req = Plack::Request->new( $env );

    my $dbh = open_db( $cfg->{ db }->{ queue } );

    # Добавляем запрос в очередь
    my $c = $dbh->prepare( 'INSERT INTO query_queue (query) VALUES (?) RETURNING id' );
    $c->execute( $req->raw_body );
    my ($query_id) = $c->fetchrow_array();
    $c->finish;

    # Возвращаем id нового запроса
    my $res = $req->new_response(200);
    $res->body( $query_id );

    return $res->finalize();
};

my $result = sub {
    my $env = shift;
    my $req = Plack::Request->new( $env );
    my $params = $req->parameters();

    my $dbh = open_db( $cfg->{ db }->{ queue } );

    # Проверяем в параметре наличие query_id
    if ( !$params->{ query_id } || $params->{ query_id } !~ /^[0-9]+$/ ) {
        my $res = $req->new_response(452);
        $res->body( "ERROR: 'query_id' parameter must be present and should be integer!" );
        return $res->finalize();
    }

    # Проверяем статус запроса
    my $c = $dbh->prepare('SELECT status, notification FROM query_queue WHERE id = ?')
    $c->execute( $params->{ query_id } );
    my ($status, $notification) = $c->fetchrow_array();
    $c->finish;

    # Если не найдено запроса с таким ID
    if ( !defined($status) ) {
        my $res = $req->new_response(404);
        $res->body( "ERROR: query with id = ".$params->{ query_id }." not found!" );
        return $res->finalize();
    }

    # Если запрос завершился с ошибкой
    if ( $status == 3) {
        my $res = $req->new_response(406);
        $res->body( "ERROR: query ".$params->{ query_id }." finished with error: ".$notification );
        return $res->finalize();
    }

    # Если запрос еще не обработан
    if ( $status < 2 ) {
        my $res = $req->new_response(422);
        $res->body( "ERROR: query ".$params->{ query_id }." not finished!" );
        return $res->finalize();
    }

    # Если запрос обработан - формируем из данных результата CSV-файл
    my $filename = "invoices_query_".$params->{ query_id }.".csv"
    $c = $dbh->prepare('SELECT period, owner_inn, owner_name, type, contractor_inn, contractor_name, date, number
                        FROM query_results
                        WHERE query_id = ?');
    $c->execute( $params->{ query_id } );
    if ( $env->{'psgi.streaming'} ) {
        # Формируем стриминговый ответ
        return sub {
            my $responder = shift;
            my $writer = $responder->([200,
                [
                    'Content-Type' => 'text/csv',
                    'Content-Disposition' => 'attachment; filename="'.$filename.'"',
                ]
            ]);

            $writer->write("period;owner_inn;owner_name;type;contractor_inn;contractor_name;date;number\n");

            while (my @row = $c->fetchrow_array()) {
                $writer->write(join(';', @row)."\n");
            };
            $c->finish;
            $writer->close();
        };
    }

    # Если не доступен стриминг - формируем ответ через временный файл
    my $tmp = new File::Temp( UNLINK => 1 );
    print $tmp "period;owner_inn;owner_name;type;contractor_inn;contractor_name;date;number\n"
    while (my @row = $c->fetchrow_array()) {
        print $tmp join(';', @row)."\n";
    };
    $c->finish;

    my $res = $req->new_response(200);
    $res->headers([
      'Content-Type' => 'text/csv',
      'Content-Disposition' => 'attachment; filename="'.$filename.'"',
    ]);
    seek $tmp, 0, 0;
    $res->body($tmp);
    return $res->finalize();
};

my $main_app = builder {
    mount "/request"    => builder { $query };
    mount "/result"     => builder { $result };
};
