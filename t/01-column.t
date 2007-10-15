use strict;
use warnings;
use Test::More tests => 12;
use Test::Memory::Cycle;

BEGIN {
    use_ok('SQL::DB::Schema::Column');
}

can_ok('SQL::DB::Schema::Column', qw(
    new
    table
    name
    type
    type_pg
    bind_type
    bind_type_pg
    null
    default
    unique
    primary
    auto_increment
    references
    sql_default
    sql
));

my $col = SQL::DB::Schema::Column->new(
{    name    => 'testcol',
    type    => 'INTEGER',
    type_pg  => 'PGINTEGER',
    null    => 1,
    default => 5,
    unique  => 1,
    primary => 1,
});

isa_ok($col, 'SQL::DB::Schema::Column');
is($col->name, 'testcol', 'name');
is($col->type, 'INTEGER', 'type');
ok($col->null == 1, 'null');
ok($col->default == 5, 'default');
ok($col->unique == 1, 'unique');
ok($col->primary == 1, 'primary');
ok(!defined($col->auto_increment), 'auto_increment');
like($col->sql, qr/testcol\s+INTEGER\s+NULL DEFAULT 5 UNIQUE/, 'SQL');

memory_cycle_ok($col);


