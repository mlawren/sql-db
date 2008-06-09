#!/usr/bin/perl
use strict;
use warnings;
use lib 't/lib';
use Test::More tests => 16;
use Test::Memory::Cycle;
use_ok('SQL::DB', 'define_tables', 'count');
use_ok('SQLDBTest');

my $db = SQLDBTest->new;

isa_ok($db, 'SQL::DB');
memory_cycle_ok($db, 'memory cycle');

my $dbh = $db->test_connect;
isa_ok($dbh, 'DBI::db');
ok(1, $db->{sqldb_dbi});
ok($dbh eq $db->dbh, 'same handle');
ok($db->deploy(), 'deploy');
ok($db->deploy(), 're-deploy');

ok($db->create_seq('test'), "Sequence test created");

ok($db->seq('test') == 1, 'seq1');
ok($db->seq('test') == 2, 'seq2');
ok($db->seq('test') == 3, 'seq3');
is_deeply([$db->seq('test',2)],[4,5], 'multiple sequence 1');
is_deeply([$db->seq('test',5)],[6,7,8,9,10], 'multiple sequence 2');

memory_cycle_ok($db, 'memory cycle');

