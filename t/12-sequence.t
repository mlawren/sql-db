use strict;
use warnings;
use Test::More;
use Test::Database;
use Cwd;
use File::Temp qw/tempdir/;
use SQL::DB;
use SQL::DBx::SQLite;
use SQL::DBx::Deploy;    # Remove this stuff

my $cwd;
BEGIN { $cwd = getcwd }

my $subs;

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
    }

    my ( $dsn, $user, $pass ) = $handle->connection_info;

    my $db = SQL::DB->new(
        dsn      => $dsn,
        username => $user,
        password => $pass,
    );

    $db->sqlite_create_function_nextval;
    $db->sqlite_create_function_currval;

    eval { $db->conn->dbh->do('DROP SEQUENCE seq_testseq'); };

    eval { $db->conn->dbh->selectrow_array("SELECT nextval('testseq')") };
    ok $@, 'exception on no sequences func';

    eval { $db->nextval('testseq') };
    ok $@, 'exception on no sequences method' . $@;

    $db->sqlite_create_sequence('testseq');

    eval { $db->conn->dbh->selectrow_array("SELECT nextval('JUNK')") };
    ok $@, 'exception on non-existent func';

    eval { $db->nextval('JUNK') };
    ok $@, 'exception on non-existent method' . $@;

    my ( $id1, $id2, $id3, $id4, $id5 );

    $id1 = $db->nextval('testseq');
    ok $id1, 'nextval:' . $id1;

    $id2 = $db->nextval('testseq');
    ok $id2 > $id1, "$id2 > $id1";

    $id3 = ( $db->conn->dbh->selectrow_array("SELECT nextval('testseq')") )[0];
    ok $id3 > $id2, "$id3 > $id2 for nextval builtin";

    $id4 = ( $db->conn->dbh->selectrow_array("SELECT currval('testseq')") )[0];
    ok $id4 == $id3, "$id4 == $id3 for currval builtin";

    $id5 = ( $db->currval('testseq') )[0];
    ok $id4 == $id5, "$id4 == $id3 for currval";
}

done_testing();

# So that File::Temp doesn't complain if it can't remove $tempdir when
# $tempdir goes out of scope;
END {
    chdir $cwd;
}

1;
