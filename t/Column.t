use strict;
use warnings;
use Test::More tests => 23;
use Scalar::Util qw(refaddr);

BEGIN { use_ok('SQL::DB::Column');}


can_ok('SQL::DB::Column', qw(new table name primary type null default unique auto_increment references));

my $col;
my $coldef = [
    name    => 'title',
    primary => 1,
    type    => 'VARCHAR(255)',
    null    => 1,
    default => 'my default',
    unique  => 1,
    auto_increment => 1,
#    references => undef,
];

$col = SQL::DB::Column->new;
isa_ok($col, 'SQL::DB::Column');


while (my $key = shift @{$coldef}) {
    my $val = shift @{$coldef};
    $col->$key($val);
    is($col->$key, $val, "attribute $key matches");
}

is($col->bind_values, ('my default'), 'bind_values match');

eval {$col->table('scalar')};
like($@, qr/table must be a/, 'table type checking');

eval {$col->references('scalar')};
like($@, qr/reference must be a/, 'reference type checking');

is($col->sql,
'title           VARCHAR(255)   NULL DEFAULT ? AUTO_INCREMENT UNIQUE PRIMARY',
'Good SQL generation');

my $coldef2 = [
    name    => 'title',
    primary => 1,
    type    => 'VARCHAR(255)',
    null    => 1,
    default => 'my default',
    unique  => 1,
    auto_increment => 1,
    references => $col,
];

my $col2 = SQL::DB::Column->new;
isa_ok($col2, 'SQL::DB::Column');

while (my $key = shift @{$coldef2}) {
    my $val = shift @{$coldef2};
    $col2->$key($val);
    if (!refaddr($val)) {
        is($col2->$key, $val, "attribute $key matches");
    }
    else {
        ok(refaddr($col2->$key) eq refaddr($val), "object $key matches");
    }
}


