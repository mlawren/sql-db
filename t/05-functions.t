use strict;
use warnings;
use Test::More tests => 37;
require 't/Schema.pm';

use_ok('SQL::DB::Function');
use_ok('SQL::DB::Schema');

can_ok('SQL::DB::Function', qw/
    coalesce
    count
    max
    min
    sum
    cast
    now
    nextval
    currval
    setval
/);

SQL::DB::Function->import(qw/
    coalesce
    count
    max
    min
    sum
    cast
    now
    nextval
    currval
    setval
/);


my $s = SQL::DB::Schema->new(Schema->Artist);
my $artist = $s->table('artists')->arow;

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
    [cast($artist->name->as('something')),
        'CAST(t0.name AS something)'],
    [now(),
        'NOW()'],
    [now()->as('now'),
        'NOW() AS now'],
    [nextval('length'),
        "nextval('length')"],
    [currval('length'),
        "currval('length')"],
    [setval('length', 1),
        "setval('length', 1)"],
    [setval('length', 1, 1 ),
        "setval('length', 1, true)"],
    [setval('length', 1, 0 ),
        "setval('length', 1, false)"],

    ){

    isa_ok($t->[0], 'SQL::DB::Expr');
    is($t->[0], $t->[1], $t->[1]);
}


