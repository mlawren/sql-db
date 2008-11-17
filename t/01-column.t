use strict;
use warnings;
use Test::More tests => 15;
use Test::Memory::Cycle;

BEGIN {
    use_ok('SQL::DB::Schema::Column');
}

can_ok('SQL::DB::Schema::Column', qw(
    new
    table
    name
    type
    type_Pg
    bind_type
    bind_type_Pg
    null
    default
    unique
    primary
    auto_increment
    references
    set
    inflate
    deflate
    sql_default
    sql
));

my $col = SQL::DB::Schema::Column->new(
{    name    => 'testcol',
    type    => 'INTEGER',
    type_Pg  => 'PGINTEGER',
    type_mysql  => 'MYSQLINTEGER',
    null    => 1,
    default => 5,
    unique  => 1,
    set     => sub{1},
    inflate => sub{1},
    deflate => sub{1},
    primary => 1,
});

isa_ok($col, 'SQL::DB::Schema::Column');
is($col->name, 'testcol', 'name');
is($col->type, 'INTEGER', 'type');
ok($col->null == 1, 'null');
ok($col->default == 5, 'default');
ok($col->unique == 1, 'unique');
ok($col->primary == 1, 'primary');
like($col->set, qr/CODE/, 'set');
like($col->inflate, qr/CODE/, 'inflate');
like($col->deflate, qr/CODE/, 'deflate');
ok(!defined($col->auto_increment), 'auto_increment');
like($col->sql, qr/testcol\s+INTEGER\s+NULL DEFAULT 5 UNIQUE/, 'SQL');

memory_cycle_ok($col);


