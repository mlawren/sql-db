use strict;
use warnings;
use Test::More tests => 11;
BEGIN {
    use_ok('SQL::DB::Column');
}

can_ok('SQL::DB::Column', qw(
    new
    table
    name
    type
    null
    default
    unique
    primary
    auto_increment
    references
    sql_default
    sql
));

my $col = SQL::DB::Column->new(
{    name    => 'testcol',
    type    => 'INTEGER',
    null    => 1,
    default => 5,
    unique  => 1,
    primary => 1,
});

isa_ok($col, 'SQL::DB::Column');
like($col->name, qr/testcol/, 'name');
like($col->type, qr/INTEGER/, 'type');
ok($col->null == 1, 'null');
ok($col->default == 5, 'default');
ok($col->unique == 1, 'unique');
ok($col->primary == 1, 'primary');
ok(!defined($col->auto_increment), 'auto_increment');
like($col->sql, qr/testcol\s+INTEGER\s+NULL DEFAULT 5 UNIQUE/, 'SQL');



