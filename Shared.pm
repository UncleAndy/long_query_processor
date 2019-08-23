package Shared;
require Exporter;
our @EXPORT_OK = qw(open_db);

use strict;
use warnings FATAL => 'all';

use DBI;

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

1;