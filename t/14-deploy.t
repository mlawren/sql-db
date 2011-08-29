#!/usr/bin/perl
use strict;
use warnings;
use Test::More;
use Test::Database;
use Cwd;
use File::Temp qw/tempdir/;
use SQL::DB;
use SQL::DB::Deploy;

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
}

done_testing();

# So that File::Temp doesn't complain if it can't remove $tempdir when
# $tempdir goes out of scope;
END {
    chdir $cwd;
}

1;
