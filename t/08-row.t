use strict;
use warnings;
use Test::More tests => 7;

use_ok('SQL::DB::Row');
can_ok('SQL::DB::Row', qw/
    make_class_from
    new
/);


my $class = SQL::DB::Row->make_class_from(qw/col1 col2 col3/);
is($class, 'SQL::DB::Row::col1_col2_col3', 'class name');

my $new = $class->new([qw(val1 val2 val3)]);
isa_ok($new, 'SQL::DB::Row::col1_col2_col3');

is($new->col1, 'val1', 'col/val 1');
is($new->col2, 'val2', 'col/val 2');
is($new->col3, 'val3', 'col/val 3');

