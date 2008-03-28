use strict;
use warnings;
use Test::More tests => 25;
use Test::Exception;
use Test::Memory::Cycle;

use_ok('SQL::DB', 'define_tables', 'count');
require_ok('t/TestLib.pm');

define_tables(TestLib->Artist);

my $db = SQL::DB->new();
isa_ok($db, 'SQL::DB');
memory_cycle_ok($db, 'memory cycle');

$db->connect(
    TestLib->dbi,undef,undef,
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

my $a1 = Artist->new(id => 1, name => 'artist1');
my $a2 = Artist->new(id => 2, name => 'artist2');
my $a3 = Artist->new(id => 3, name => 'artist3');
my $a4 = Artist->new(id => 4, name => 'artist4');
my $artists = $db->arow('artists');

ok($db->txn(sub {
    $db->insert($a1);
}), 'transaction insert success');

is($db->fetch1(
    select => count($artists->id)->as('acount'),
    from   => $artists,
)->acount,1, 'select 1');

my $res = $db->txn(sub {
    $db->insert($a1);
});
ok(!$res, "transaction insert duplicate:");

is($db->fetch1(
    select => count($artists->id)->as('acount'),
    from   => $artists,
)->acount,1, 'still select 1');

ok(!$db->txn(sub {
    $db->insert($a2);
    $db->insert($a1);
}), 'transaction insert duplicate with non-dup');

is($db->fetch1(
    select => count($artists->id)->as('acount'),
    from   => $artists,
)->acount,1, 'still select 1');

$res = $db->txn(sub {
    $db->insert($a2);
    $db->insert($a3);
});

ok($res, 'transaction insert 2 and 3');

is($db->fetch1(
    select => count($artists->id)->as('acount'),
    from   => $artists,
)->acount,3, 'select 3');

$res = $db->txn(sub {
    $db->txn(sub {
        $db->insert($a4);
    });
});

ok($res, 'nested transaction insert 4');

memory_cycle_ok($db, 'memory cycle');

$res = $db->txn(sub {
    $db->insert($a4);
});
ok(!$res, "transaction insert duplicate 4");

my $subref = sub {
    $db->txn(sub{
        $db->insert($a4);
    });
};

#$SQL::DB::DEBUG=1;
$res = $db->txn($subref);

ok(!$res, 'nested transaction insert 4 again');
