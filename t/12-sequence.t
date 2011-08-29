use strict;
use warnings;
use Test::More;
use Test::Database;
use Cwd;
use File::Temp qw/tempdir/;
use SQL::DB;
use SQL::DB::Sequence;
use SQL::DB::Deploy;    # Remove this stuff

can_ok(
    'SQL::DB', qw/
      create_sequence
      nextval
      /
);

my $cwd;
BEGIN { $cwd = getcwd }

my $subs;

my @handles = Test::Database->handles(qw/ SQLite Pg mysql /);

if ( !@handles ) {
    plan skip_all => "No database handles to test with";
}

my $deploy = {
    SQLite => [
        { 'sql'  => 'create table test (id integer, name varchar)' },
        { 'perl' => '1' },
    ],
    Pg => [
        { 'sql'  => 'create table test (id integer, name varchar)' },
        { 'perl' => '2' },
    ],
    mysql => [
        { 'sql'  => 'create table test (id integer, name varchar)' },
        { 'perl' => '3' },
    ],
};

my $tempdir;
foreach my $handle (@handles) {
    chdir $cwd || die "chdir: $!";
    $tempdir = tempdir( CLEANUP => 1 );
    chdir $tempdir || die "chdir: $!";

    my ( $dsn, $user, $pass ) = $handle->connection_info;

    my $db = SQL::DB->new(
        dsn    => $dsn,
        dbuser => $user,
        dbpass => $pass,
    );

    isa_ok( $db, 'SQL::DB' );

    # Clean up any previous runs (mostly for Pg's sake)
    eval { $db->conn->dbh->do('DROP TABLE _sqldb'); };
    eval { $db->conn->dbh->do('DROP TABLE test'); };
    eval { $db->conn->dbh->do('DROP TABLE test2'); };
    eval { $db->conn->dbh->do('DROP SEQUENCE seq_test'); };

    my $ret;

    ok $ret = $db->deploy( 'sqldb-test', $deploy ), 'deployed to ' . $db->dbd;
    ok $ret = $db->deploy( 'sqldb-test', $deploy ), 're-deploy same';

    my $prev_id = $db->last_deploy_id('sqldb-test');
    ok $prev_id == $ret, 'return values';

    push( @{ $deploy->{ $db->dbd } }, { sql => 'create table test2(id int)' } );
    ok $ret = $db->deploy( 'sqldb-test', $deploy ), 're-deploy more';

    my $last_id = $db->last_deploy_id('sqldb-test');
    ok $last_id == $ret, 'return values';

    ok $last_id > $prev_id, 'upgrade occurred';

  SKIP: {
        skip 'Deploy YAML not enabled', 1 unless eval { require YAML::Tiny };
        ok $db->deploy( 'sqldb-test', YAML::Tiny::Dump($deploy) ),
          'yaml deploy';
    }

    $db->create_sequence('test');
    my $id = $db->nextval('test');
    ok $id, 'nextval';

    ok $db->insert_into( 'test', values => { id => $id, name => 'Mark' } ),
      'insert';

    ok $db->insert_into(
        'test', values => { id => $db->nextval('test'), name => 'Mark2' }
      ),
      'insert';

    my @res = $db->select( [ 'id', 'name' ], from => 'test', );

    ok @res == 2, 'select many';
    can_ok $res[0], qw/id name/;

    my $res = $db->select(
        [ 'id', 'name' ],
        from  => 'test',
        where => { id => 1 },
    );

    is $res->id,   1,      'res id';
    is $res->name, 'Mark', 'res name';

    my $test = $db->srow('test');

    # These tests are repeated to make sure we are closing the
    # statement handle at the end.
    foreach ( 1 .. 2 ) {
        my $iter = $db->iter(
            select => [ $test->id, $test->name ],
            from   => $test,
            where  => $test->id == 1,
            limit  => 1,
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
            select => [ $test->id, $test->name ],
            from   => $test,
        );

        my @rows = $iter->all;
        ok !$iter->next, 'next is undef';
        is ref $rows[0], $iter->class, 'row is ' . $iter->class;
    }
}

done_testing();

# So that File::Temp doesn't complain if it can't remove $tempdir when
# $tempdir goes out of scope;
END {
    chdir $cwd;
}

1;
