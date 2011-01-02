#!/usr/bin/perl
use strict;
use warnings;
use lib 't/lib';
use Test::More;
use Test::Database;
use SQL::DB;

my $subs;

my @handles = Test::Database->handles(qw/ SQLite Pg mysql /);

plan tests => 1 * @handles;

foreach my $handle (@handles) {
    my ( $dsn, $user, $pass ) = $handle->connection_info;

    my $db = SQL::DB->new(
        dsn    => $dsn,
        dbuser => $user,
        dbpass => $pass,
    );

    isa_ok( $db, 'SQL::DB' );
}

done_testing();

1;
