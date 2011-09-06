use strict;
use warnings;
use Test::More;
use Test::Database;
use Cwd;
use File::Temp qw/tempdir/;
use SQL::DB;
use SQL::DB::X::Sequence;
use SQL::DB::X::Deploy;    # Remove this stuff

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

    $db->create_sequence('test');
    my $id = $db->nextval('test');
    ok $id, 'nextval';
}

done_testing();

# So that File::Temp doesn't complain if it can't remove $tempdir when
# $tempdir goes out of scope;
END {
    chdir $cwd;
}

1;
