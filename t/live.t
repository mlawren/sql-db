#!/usr/bin/perl
use strict;
use warnings;
use lib 't/lib';
use Test::More;
use Test::Database;
use SQL::DB::Test;
use Benchmark qw(:all);

my $subs;
my $tests;

my @handles = Test::Database->handles( qw/ SQLite Pg mysql / );

if ( @handles > 1 ) {
    foreach my $h ( @handles ) {
        $tests->{$h->dbd} = SQL::DB::Test->new;
        $tests->{$h->dbd}->connect($h->dsn, $h->username, $h->password);

        $subs->{$h->dbd} = sub {
            $tests->{$h->dbd}->runtests;
        };
    }

    my $results = timethese( 1, $subs, 'none' );
    cmpthese( $results );

    diag <<EOF;

The above benchmark results are RUBBISH unless you increase the count
in the test file to something reasonable, and are aware of the various
parts of your environment that affect such test results and are aware
that most of these tests drop and deploy tables each run. I inserted
the Benchmark test output mostly to see how common it is for CPAN
testers to be using Test::Database.

EOF
}
else {
    my $dsn = 'dbi:SQLite:dbname=:memory:';
    diag('DSN: '. $dsn);
    my $t = SQL::DB::Test->new;
    $t->connect($dsn);
    $t->runtests;
}

1;
