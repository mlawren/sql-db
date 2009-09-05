use strict;
use warnings;
use Test::More tests => 66;

use SQL::DB::Expr;
#', qw/ AND OR /);

#BEGIN{
#SQL::DB::Expr->import(qw/AND OR/);
#}

can_ok('SQL::DB::Expr', qw/
    new
    bind_values
    multi
/);


# Of course one should never do this, because Expr is kind of
# an abstract class. But for testing...
my $e1 = SQL::DB::Expr->new(val => 'e1');
my $e2 = SQL::DB::Expr->new(val => 'e2');
my $ce1 = ($e1 .AND. $e2);
my $ce2 = ($e1 .OR. $e2);
my $ce3 = ($ce1 .AND. $ce2);

is($e1->as_string, 'e1', 'e1 is e1');


# Just the basic operators
foreach (
    [$e1 == 1, 'e1 = ?'],
    [$e1 == 1, 'e1 = ?'],
    [$e1 eq 1, 'e1 = ?'],
    [$e1 != 1, 'e1 != ?'],
    [$e1 ne 1, 'e1 != ?'],
    [!$e1,     'NOT (e1)'],
    [$e1 .AND. 1,  'e1 AND ?'],
    [$e1 .OR. 1,  'e1 OR ?'],
    [$e1 < 1,  'e1 < ?'],
    [$e1 > 1,  'e1 > ?'],
    [$e1 >= 1, 'e1 >= ?'],
    [$e1 <= 1, 'e1 <= ?'],
    [1 > $e1,  '? > e1'],
    [1 < $e1,  '? < e1'],
    [1 >= $e1, '? >= e1'],
    [1 <= $e1, '? <= e1'],
    [$e1 + 1,  'e1 + ?'],
    [$e1 - 1,  'e1 - ?'],
    [1 - $e1,  '? - e1'],
    [$e1 * 1,  'e1 * ?'],
    [$e1 / 1,  'e1 / ?'],
    [1 / $e1,  '? / e1'],
    [$e1 == $e2, 'e1 = e2'],
    [$e1 == $e2, 'e1 = e2'],
    [$e1 eq $e2, 'e1 = e2'],
    [$e1 != $e2, 'e1 != e2'],
    [$e1 ne $e2, 'e1 != e2'],
    [$e1 .AND. $e2,  'e1 AND e2'],
    [!$e1,        'NOT (e1)'],
    [$e1 .OR. $e2,  'e1 OR e2'],
    [$e1 < $e2,  'e1 < e2'],
    [$e1 > $e2,  'e1 > e2'],
    [$e1 >= $e2, 'e1 >= e2'],
    [$e1 <= $e2, 'e1 <= e2'],
    [$e1 + $e2,  'e1 + e2'],
    [$e1 - $e2,  'e1 - e2'],
    [$e1->expr_and(1), 'e1 AND ?'],
    [$e1->expr_or(1),  'e1 OR ?'],
    [$e1->concat($e2),  'e1 || e2'],
    [$e1->is_null,  'e1 IS NULL'],
    [$e1->is_not_null,  'e1 IS NOT NULL'],
    [$e1->in($e1,$e2),  'e1 IN (e1, e2)'],
    [$e1->not_in($e1,$e2),  'e1 NOT IN (e1, e2)'],
    [$e1->between($e1,$e2),  '(e1 BETWEEN e1 AND e2)'],
    [($e1 == $e2) .AND. ($e1 == $e2), 'e1 = e2 AND e1 = e2'],
    [($e1 == $e2) .OR. ($e1 == $e2), 'e1 = e2 OR e1 = e2'],
    [($e1 == $e2) .AND. !($e1 == $e2), 'e1 = e2 AND NOT (e1 = e2)'],
    [($e1 == $e2) .OR. !($e1 == $e2), 'e1 = e2 OR NOT (e1 = e2)'],
    [!($e1 == $e2) .AND. ($e1 == $e2), 'NOT (e1 = e2) AND e1 = e2'],
    [!($e1 == $e2) .OR. ($e1 == $e2), 'NOT (e1 = e2) OR e1 = e2'],
    [!($e1 == $e2) .AND. !($e1 == $e2), 'NOT (e1 = e2) AND NOT (e1 = e2)'],
    [!($e1 == $e2) .OR. !($e1 == $e2), 'NOT (e1 = e2) OR NOT (e1 = e2)'],
    [!(!($e1 == $e2) .OR. !($e1 == $e2)), 'NOT (NOT (e1 = e2) OR NOT (e1 = e2))'],
    [(($e1 == $e2) .OR. !($e1 == $e2)) .AND. !(!($e1 == $e2) .OR. !($e1 == $e2)),
      '(e1 = e2 OR NOT (e1 = e2)) AND NOT (NOT (e1 = e2) OR NOT (e1 = e2))'],
    [$e1 == ($e1 .OR. ($e2 .AND. ($e1 == $e2))), 'e1 = e1 OR (e2 AND e1 = e2)'],

    ) {

    is($_->[0], $_->[1], $_->[1]);
}

is_deeply([($e1 + $e1)->bind_values], [], 'bind values');
is_deeply([($e1 + 1)->bind_values], [1], 'bind values');


# Now some combinations


foreach (
    [$e1 .AND. $ce2, 'e1 AND (e1 OR e2)'],
    [($e1 .AND. $e2) .OR. ($e1 .AND. $e2), '(e1 AND e2) OR (e1 AND e2)'],
    [$ce1 .AND. $ce2, 'e1 AND e2 AND (e1 OR e2)'],
    [$ce1 .AND. !$ce2, 'e1 AND e2 AND NOT (e1 OR e2)'],
    [$ce2 .AND. $ce3, '(e1 OR e2) AND e1 AND e2 AND (e1 OR e2)'],
    [!($ce2 .AND. $ce3), 'NOT ((e1 OR e2) AND e1 AND e2 AND (e1 OR e2))'],
    [$ce2 .AND. !$ce3, '(e1 OR e2) AND NOT (e1 AND e2 AND (e1 OR e2))'],
    ) {

    is($_->[0], $_->[1], $_->[1]);
}



