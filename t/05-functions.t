use strict;
use warnings;
use Test::More tests => 21;
require 't/Schema.pm';

use_ok('SQL::DB::Function');
use_ok('SQL::DB::Schema');

can_ok('SQL::DB::Function', qw/
    coalesce
    count
    max
    min
    sum
/);

SQL::DB::Function->import(qw/
    coalesce
    count
    max
    min
    sum
/);


#my $sql = SQL::DB::Schema->new(Schema->All);
#my $artist = Artist->arow;

foreach my $t (
    [coalesce('col1', 'col2')->as('col'),
        'COALESCE(col1, col2) AS col' ],
    [count('*'),
        'COUNT(*)'],
    [count('id')->as('count_id'),
        'COUNT(id) AS count_id'],
    [max('length'),
        'MAX(length)'],
    [max('length')->as('max_length'),
        'MAX(length) AS max_length'],
    [min('length'),
        'MIN(length)'],
    [min('length')->as('min_length'),
        'MIN(length) AS min_length'],
    [sum('length'),
        'SUM(length)'],
    [sum('length')->as('sum_length'),
        'SUM(length) AS sum_length'],

    ){

    isa_ok($t->[0], 'SQL::DB::Function');
    is($t->[0], $t->[1], $t->[1]);
}


