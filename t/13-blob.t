use strict;
use warnings;
use Test::More tests => 7;
use Test::Exception;
use Test::Memory::Cycle;
use utf8;

use_ok('SQL::DB', 'define_tables', 'count');
require_ok('t/TestLib.pm');

define_tables(TestLib->Btable);

my $db = SQL::DB->new();
$db->connect(
    TestLib->dbi,undef,undef,
#    'dbi:Pg:dbname=test;port=5433', 'rekudos', 'rekudos',
    {unicode => 1, pg_enable_utf8 => 1, PrintError => 0, RaiseError => 1},
);
ok(1, 'connected');

$db->deploy;
ok(1, 'deployed');

ok($db->create_seq('test'), "Sequence test created");

memory_cycle_ok($db, 'memory cycle');

my $bindata = {
    one => 'Hello world, Καλημέρα κόσμε, コンニチハ',
};

my $bin = Btable->new(bincol => $bindata);


$db->insert($bin);

my $btable = $db->arow('btable');

TODO: {
    local $TODO = 'DBD::SQLite bug still sets utf8 on BLOB columns';
    my $new;
    eval {
        $new = $db->fetch1(
            select => [$btable->bincol],
            from   => $btable,
        );
    };
    ok($new);
#    is_deeply($bindata, $new->bincol, 'binary data stored and fetched');
}
