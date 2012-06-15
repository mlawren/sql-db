use strict;
use warnings;
use Test::More;
use Test::Database;
use Cwd;
use File::Temp qw/tempdir/;
use Path::Class;
use SQL::DB ':all';
use SQL::DBx::Deploy;
use FindBin qw/$Bin/;
use lib "$Bin/lib";

BEGIN {
    unless ( eval { require YAML; } ) {
        plan skip_all => "Feature Deploy YAML not enabled";
    }
}

can_ok( 'SQL::DB', qw/deploy last_deploy_id/ );

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

    my ( $dsn, $user, $pass ) = $handle->connection_info;

    if ( $handle->dbd eq 'SQLite' ) {
        $handle->driver->drop_database( $handle->name );
        $handle->driver->drop_database( $handle->name . '.seq' );
    }

    my $db = SQL::DB->new(
        dsn      => $dsn,
        username => $user,
        password => $pass,
    );

    isa_ok( $db, 'SQL::DB' );

    # Clean up any previous runs (mostly for Pg's sake)
    eval { $db->conn->dbh->do('DROP TABLE film_actors'); };
    eval { $db->conn->dbh->do('DROP TABLE actors'); };
    eval { $db->conn->dbh->do('DROP TABLE films'); };
    eval {
        $db->conn->dbh->do( 'DROP TABLE ' . $SQL::DBx::Deploy::DEPLOY_TABLE );
    };
    eval { $db->conn->dbh->do('DROP SEQUENCE seq_test'); };

    my $ret;
    my $prev_id;

    $prev_id = $db->last_deploy_id;
    is $prev_id, 0, 'Nothing deployed yet: ' . $prev_id;

    my $file1 = file( $Bin, 'deploy',  $handle->dbd . '.yaml' );
    my $file2 = file( $Bin, 'deploy2', $handle->dbd . '.yaml' );
    $ret = $db->deploy_file($file1);
    is $ret, 3, 'deployed to ' . $ret;

    $prev_id = $db->last_deploy_id;
    is $prev_id, 3, 'last id check';

    $ret = $db->deploy_file($file1);
    is $ret, 3, 'still deployed to ' . $ret;

    $prev_id = $db->last_deploy_id;
    is $prev_id, 3, 'still last id check';

    $ret = $db->deploy_file($file2);
    is $ret, 4, 'upgraded to ' . $ret;

    ok $db->do(
        insert_into => \'actors(id,name)',
        sql_values( 1, 'Mark' )
      ),
      'insert';

    ok $db->do(
        insert_into => sql_table( 'actors', qw/id name/ ),
        sql_values( 2, 'Mark2' )
      ),
      'insert';

    my $actors = $db->irow('actors');
    ok $db->do(
        insert_into => $actors->(qw/id name/),
        sql_values( bv(3), 'Mark3' )
      ),
      'insert';

    $actors = $db->srow('actors');

    my @res = $db->fetch(
        select => [ $actors->id, $actors->name ],
        from   => $actors,
    );

    ok @res == 3, 'select many';
    can_ok $res[0], qw/id name/;

    my $res = $db->fetch1(
        select => [ $actors->id, $actors->name ],
        from   => $actors,
        where  => $actors->id == 1,
    );

    is $res->id,   1,      'res id';
    is $res->name, 'Mark', 'res name';

    # These tests are repeated to make sure we are closing the
    # statement handle at the end.
    foreach ( 1 .. 2 ) {
        my $iter = $db->iter(
            select => [ $actors->id, $actors->name ],
            from   => $actors,
            where => ( $actors->id == 1 ) . AND . ( $actors->name != 'Julie' ),
            limit => 1,
        );

        isa_ok $iter, 'SQL::DB::Iter';
        isa_ok $iter->sth, 'DBI::st';
        ok $iter->class,   $iter->class;

        my $row = $iter->next;
        is ref $row, $iter->class, 'row is ' . $iter->class;
        is $row->id,   1,      'rowid';
        is $row->name, 'Mark', 'row name';

        ok !$iter->next, 'next is undef';

        $iter = $db->iter(
            select => [ $actors->id, $actors->name ],
            from   => $actors,
        );

        my @rows = $iter->all;
        ok !$iter->next, 'next is undef';
        is ref $rows[0], $iter->class, 'row is ' . $iter->class;
    }

    is sql_cast( $actors->id, as => 'char' )->_as_string,
      'CAST( actors0.id AS char )', 'CAST';

    ok $db->delete(
        from  => 'actors',
        where => {},
      ),
      'delete';

    ok $db->insert(
        into   => 'actors',
        values => { id => 1, name => 'Mark' }
      ),
      'insert';

    ok $db->insert(
        into   => 'actors',
        values => { id => 2, name => 'Mark2' }
      ),
      'insert';

    @res = $db->select( [ 'id', 'name' ], from => 'actors', );

    ok @res == 2, 'select many';
    can_ok $res[0], qw/id name/;

    $res = $db->select(
        [ 'id', 'name' ],
        from  => 'actors',
        where => { id => 1 },
    );

    is $res->id,   1,      'res id';
    is $res->name, 'Mark', 'res name';
}

done_testing();

# So that File::Temp doesn't complain if it can't remove $tempdir when
# $tempdir goes out of scope;
END {
    chdir $cwd;
}

1;

