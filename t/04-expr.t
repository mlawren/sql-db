use strict;
use warnings;
use Test::More tests => 52;

use_ok('SQL::DB::Schema::Expr');

can_ok('SQL::DB::Schema::Expr', qw/
    new
    bind_values
    multi
/);


# Of course one should never do this, because Expr is kind of
# an abstract class. But for testing...
my $e1 = SQL::DB::Schema::Expr->new('e1');
my $e2 = SQL::DB::Schema::Expr->new('e2');
my $ce1 = SQL::DB::Schema::Expr->new($e1 & $e2);
my $ce2 = SQL::DB::Schema::Expr->new($e1 | $e2);
my $ce3 = $ce1 & $ce2;



# Just the basic operators
foreach (
    [$e1 == 1, 'e1 = ?'],
    [$e1 == 1, 'e1 = ?'],
    [$e1 eq 1, 'e1 = ?'],
    [$e1 != 1, 'e1 != ?'],
    [$e1 ne 1, 'e1 != ?'],
    [$e1 & 1,  'e1 AND ?'],
    [!$e1,        'NOT (e1)'],
    [$e1 | 1,  'e1 OR ?'],
    [$e1 < 1,  'e1 < ?'],
    [$e1 > 1,  'e1 > ?'],
    [$e1 >= 1, 'e1 >= ?'],
    [$e1 <= 1, 'e1 <= ?'],
    [$e1 + 1,  'e1 + ?'],
    [$e1 - 1,  'e1 - ?'],
    [$e1 == $e2, 'e1 = e2'],
    [$e1 == $e2, 'e1 = e2'],
    [$e1 eq $e2, 'e1 = e2'],
    [$e1 != $e2, 'e1 != e2'],
    [$e1 ne $e2, 'e1 != e2'],
    [$e1 & $e2,  'e1 AND e2'],
    [!$e1,        'NOT (e1)'],
    [$e1 | $e2,  'e1 OR e2'],
    [$e1 < $e2,  'e1 < e2'],
    [$e1 > $e2,  'e1 > e2'],
    [$e1 >= $e2, 'e1 >= e2'],
    [$e1 <= $e2, 'e1 <= e2'],
    [$e1 + $e2,  'e1 + e2'],
    [$e1 - $e2,  'e1 - e2'],
    [$e1->in($e1,$e2),  'e1 IN (e1, e2)'],
    [$e1->not_in($e1,$e2),  'e1 NOT IN (e1, e2)'],
    [$e1->between($e1,$e2),  '(e1 BETWEEN e1 AND e2)'],
    [($e1 == $e2) & ($e1 == $e2), 'e1 = e2 AND e1 = e2'],
    [($e1 == $e2) | ($e1 == $e2), 'e1 = e2 OR e1 = e2'],
    [($e1 == $e2) & !($e1 == $e2), 'e1 = e2 AND NOT (e1 = e2)'],
    [($e1 == $e2) | !($e1 == $e2), 'e1 = e2 OR NOT (e1 = e2)'],
    [!($e1 == $e2) & ($e1 == $e2), 'NOT (e1 = e2) AND e1 = e2'],
    [!($e1 == $e2) | ($e1 == $e2), 'NOT (e1 = e2) OR e1 = e2'],
    [!($e1 == $e2) & !($e1 == $e2), 'NOT (e1 = e2) AND NOT (e1 = e2)'],
    [!($e1 == $e2) | !($e1 == $e2), 'NOT (e1 = e2) OR NOT (e1 = e2)'],
    [!(!($e1 == $e2) | !($e1 == $e2)), 'NOT (NOT (e1 = e2) OR NOT (e1 = e2))'],
    [(($e1 == $e2) | !($e1 == $e2)) & !(!($e1 == $e2) | !($e1 == $e2)),
      '(e1 = e2 OR NOT (e1 = e2)) AND NOT (NOT (e1 = e2) OR NOT (e1 = e2))'],
    ) {

    is($_->[0], $_->[1], $_->[1]);
}

is_deeply([($e1 + $e1)->bind_values], [], 'bind values');
is_deeply([($e1 + 1)->bind_values], [1], 'bind values');


# Now some combinations


foreach (
    [$e1 & $ce2, 'e1 AND (e1 OR e2)'],
    [($e1 & $e2) | ($e1 & $e2), '(e1 AND e2) OR (e1 AND e2)'],
    [$ce1 & $ce2, 'e1 AND e2 AND (e1 OR e2)'],
    [$ce1 & !$ce2, 'e1 AND e2 AND NOT (e1 OR e2)'],
    [$ce2 & $ce3, '(e1 OR e2) AND e1 AND e2 AND (e1 OR e2)'],
    [!($ce2 & $ce3), 'NOT ((e1 OR e2) AND e1 AND e2 AND (e1 OR e2))'],
    [$ce2 & !$ce3, '(e1 OR e2) AND NOT (e1 AND e2 AND (e1 OR e2))'],
    ) {

    is($_->[0], $_->[1], $_->[1]);
}



