use strict;
use warnings;
use Test::More;
use Test::Database;
use Cwd;
use File::Temp qw/tempdir/;
use SQL::DB ':all';
use SQL::DBx::Deploy;
use SQL::DB::Schema;
use FindBin;
use lib "$FindBin::RealBin/lib";

my $cwd;
BEGIN { $cwd = getcwd }

mkdir "$FindBin::RealBin/lib/test/Schema";
ok -d "$FindBin::RealBin/lib/test/Schema", 'Directory';

my @handles = Test::Database->handles(qw/ SQLite Pg mysql /);

if ( !@handles ) {
    plan skip_all => "No database handles to test with";
}

my $tempdir;
foreach my $handle (@handles) {
    chdir $cwd || die "chdir: $!";
    $tempdir = tempdir( CLEANUP => 1 );
    chdir $tempdir || die "chdir: $!";

    if ( $handle->dbd eq 'SQLite' ) {
        $handle->driver->drop_database( $handle->name );
        $handle->driver->drop_database( $handle->name . '.seq' );
    }

    my $db = SQL::DB->connect( $handle->connection_info );

    # Clean up any previous runs (mostly for Pg's sake)
    eval { $db->conn->dbh->do('DROP TABLE film_actors'); };
    eval { $db->conn->dbh->do('DROP TABLE actors'); };
    eval { $db->conn->dbh->do('DROP TABLE films'); };
    eval {
        $db->conn->dbh->do( 'DROP TABLE ' . $SQL::DBx::Deploy::DEPLOY_TABLE );
    };
    eval { $db->conn->dbh->do('DROP SEQUENCE seq_test'); };

    $db->deploy('test::Deploy');

    my $pkg  = "test::Schema::" . $handle->dbd;
    my $file = "$FindBin::RealBin/lib/test/Schema/" . $handle->dbd . '.pm';

    unlink $file;
    ok !-e $file, "$file doesn't exist";

    my $cmd =
        "$^X $FindBin::RealBin/sqldb-schema -u " . " '"
      . $handle->username . "' " . " '"
      . $handle->dsn . "' "
      . " $pkg $file";

    open( my $fh, '|-', $cmd ) || die "open: $!";

    print $fh $handle->password . "\n";
    close $fh;

    ok -e $file, "$file generated";

    my $schema = SQL::DB::Schema->new( name => $pkg, load => 1 );
    isa_ok $schema, 'SQL::DB::Schema';

    is $schema->name, $pkg, $pkg;
}

done_testing();

# So that File::Temp doesn't complain if it can't remove $tempdir when
# $tempdir goes out of scope;
END {
    chdir $cwd;
}

1;

