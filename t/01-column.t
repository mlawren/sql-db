use strict;
use warnings;
use lib 't/lib';
use Test::More tests => 15;
use Test::Memory::Cycle;
use SQL::DB qw/Schema/;
use SQL::DB::Column;
use SQL::DB::Test::Schema;

can_ok('SQL::DB::Column', qw(
    new
    table
    name
    type
    bind_type
    primary
    null
    default
    references
    deferrable
    unique
    auto_increment
    set
    inflate
    deflate
    as_sql
));

Schema('music')->dbd('SQLite');
my $col = Schema('music')->table('btable')->column('bincol');

isa_ok($col, 'SQL::DB::Column');
is($col->name, 'bincol', 'name');

is($col->type, 'blob', 'type');
Schema('music')->dbd('Pg');
is($col->type, 'BYTEA', 'type');

ok(!$col->null, 'null');
#ok($col->default == 5, 'default');
#ok($col->unique == 1, 'unique');
#ok($col->primary == 1, 'primary');
#like($col->set, qr/CODE/, 'set');
like($col->inflate, qr/CODE/, 'inflate');
like($col->deflate, qr/CODE/, 'deflate');
ok(! $col->auto_increment, 'auto_increment');
like($col->as_sql, qr/^bincol\s+BYTEA\s+NOT NULL$/, 'SQL');

memory_cycle_ok($col);


