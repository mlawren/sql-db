use strict;
use warnings;
use Test::More;
use Test::Memory::Cycle;

BEGIN {
    if (!eval {require DBD::SQLite;1;}) {
        plan skip_all => "DBD::SQLite not installed: $@";
    }
    else {
        plan tests => 14;
    }

}
END {
    unlink "/tmp/sqldbtest.db";
}


use_ok('SQL::DB');
require_ok('t/Schema.pm');

# FIXME FILL THIS OUT
can_ok('SQL::DB', qw/
    new
    create_seq
    seq
/);

unlink "/tmp/sqldb$$.db";

my $db = SQL::DB->new();
isa_ok($db, 'SQL::DB');
memory_cycle_ok($db, 'memory cycle');

$db->connect(
    "dbi:SQLite:/tmp/sqldb$$.db",undef,undef,
#    'dbi:Pg:dbname=test;port=5433', 'rekudos', 'rekudos',
    {PrintError => 0, RaiseError => 1},
);
ok(1, 'connected');

$db->deploy;
ok(1, 'deployed');

ok($db->create_seq('test'), "Sequence test created");

ok($db->seq('test') == 1, 'seq1');
ok($db->seq('test') == 2, 'seq1');
ok($db->seq('test') == 3, 'seq1');
is_deeply([$db->seq('test',2)],[4,5], 'seq2');
is_deeply([$db->seq('test',5)],[6,7,8,9,10], 'seq5');

memory_cycle_ok($db, 'memory cycle');

