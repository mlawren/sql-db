use strict;
use warnings;
use Test::More;
use Test::Differences qw/eq_or_diff/;
use SQL::DB::Expr qw/AND OR _bval _expr_join /;

can_ok(
    'SQL::DB::Expr', qw/
      new
      _quote
      _bval
      _multi
      _expr_join
      _expr_binary
      AND
      OR
      /
);

# Of course one should never do this, because Expr is kind of
# an abstract class. But for testing...
my $e1 = SQL::DB::Expr->new( _txt => ['e1'] );
my $e2 = SQL::DB::Expr->new( _txt => ['e2'], _btype => 'e2type' );
my $ce1 = ( $e1 . AND . $e2 );
my $ce2 = ( $e1 . OR . $e2 );
my $ce3 = ( $ce1 . AND . $ce2 );
my $ce4 = ( ( $e1 != 6 ) . OR . ( $e2 < 3 ) );

my $alias = SQL::DB::Expr->new( _alias => 'alias', _txt => ['junk'] );

is( $e1->_as_string, 'e1', 'e1 is e1' );
my $x;

$x = $e1 . ' after';
is $x->_as_string, 'e1 after', 'after';

$x = $e1 . ' after';
is $x->_as_string, 'e1 after', 'after';

$x = 'before ' . $e1;
is $x->_as_string, 'before e1', 'before';

$x = $e1 . $e2;
is $x->_as_string, 'e1e2', 'e1e2';

$x = $e2 . $e1;
is $x->_as_string, 'e2e1', 'e2e1';

my $extra = ' extra ';

# Just the basic operators

foreach (
    [ _expr_join( ', ', qw/one two three/ ), 'one, two, three' ],
    [ $alias,           'alias AS alias0' ],
    [ !$e1,             'NOT e1' ],
    [ $e2 == 1,         'e2 = q{1}' ],
    [ $e1 != _bval(1),  'e1 != bv{1}::auto' ],
    [ $e1 < 1,          'e1 < q{1}' ],
    [ $e1 > 1,          'e1 > q{1}' ],
    [ $e1 >= 1,         'e1 >= q{1}' ],
    [ $e1 <= 1,         'e1 <= q{1}' ],
    [ 1 > $e2,          'q{1} > e2' ],
    [ 1 < $e1,          'q{1} < e1' ],
    [ 1 >= $e1,         'q{1} >= e1' ],
    [ 1 <= $e2,         'q{1} <= e2' ],
    [ $e1 + 1,          'e1 + q{1}' ],
    [ $e1 - 1,          'e1 - q{1}' ],
    [ 1 - $e1,          'q{1} - e1' ],
    [ $e1 * 1,          'e1 * q{1}' ],
    [ $e1 / 1,          'e1 / q{1}' ],
    [ 1 / $e1,          'q{1} / e1' ],
    [ $e1 == $e2,       'e1 = e2' ],
    [ $e1 == $e2,       'e1 = e2' ],
    [ $e1 != $e2,       'e1 != e2' ],
    [ $e1 . AND . $e2,  'e1 AND e2' ],
    [ !$e1,             'NOT e1' ],
    [ $e1 . OR . $e2,   'e1 OR e2' ],
    [ $e1 < $e2,        'e1 < e2' ],
    [ $e1 > $e2,        'e1 > e2' ],
    [ $e1 >= $e2,       'e1 >= e2' ],
    [ $e1 <= $e2,       'e1 <= e2' ],
    [ $e1 + $e2,        'e1 + e2' ],
    [ $e1 - $e2,        'e1 - e2' ],
    [ $e1->is_null,     'e1 IS NULL' ],
    [ $e1->is_not_null, 'e1 IS NOT NULL' ],
    [ $e1->in( $e1, $e2 ), 'e1 IN (e1, e2)' ],
    [ $e1->not_in( $e1, $e2 ), 'e1 NOT IN (e1, e2)' ],
    [ $e1->in( 9, 10 ), 'e1 IN (q{9}, q{10})' ],

    #    [ $e1->in( 9, _quote(10) ), 'e1 IN (q{9}, q{10})' ],
    [ $e1->in( 9, _bval(10) ), 'e1 IN (q{9}, bv{10}::auto)' ],
    [ $e1->in( 9, _bval( 10, 'x' ) ), 'e1 IN (q{9}, bv{10}::x)' ],
    [ $e2->in( 9, 10 ), 'e2 IN (q{9}, q{10})' ],
    [ $e1->not_in( 9, 10 ),        'e1 NOT IN (q{9}, q{10})' ],
    [ $e2->not_in( 9, _bval(10) ), 'e2 NOT IN (q{9}, bv{10}::e2type)' ],
    [ $e2->not_in( 9, _bval( 10, 'x' ) ), 'e2 NOT IN (q{9}, bv{10}::x)' ],

    [ $e1->between( $e1, $e2 ),       'e1 BETWEEN e1 AND e2' ],
    [ $e1->between( 13,  _bval(34) ), 'e1 BETWEEN q{13} AND bv{34}::auto' ],
    [ $e1->between( 13, _bval( 34, 'x' ) ), 'e1 BETWEEN q{13} AND bv{34}::x' ],
    [ $e1->not_between( $e1, $e2 ), 'e1 NOT BETWEEN e1 AND e2' ],

    [
        $e1->not_between( 13, _bval(34) ),
        'e1 NOT BETWEEN q{13} AND bv{34}::auto'
    ],
    [
        $e1->not_between( 13, _bval( 34, 'x' ) ),
        'e1 NOT BETWEEN q{13} AND bv{34}::x'
    ],

    [ $e2->between( 13, _bval(34) ), 'e2 BETWEEN q{13} AND bv{34}::e2type' ],
    [ $e2->between( 13, _bval( 34, 'x' ) ), 'e2 BETWEEN q{13} AND bv{34}::x' ],

    [ $e2->not_between( $e1, $e2 ), 'e2 NOT BETWEEN e1 AND e2' ],
    [
        $e2->not_between( 13, _bval(34) ),
        'e2 NOT BETWEEN q{13} AND bv{34}::e2type'
    ],
    [
        $e2->not_between( 13, _bval( 34, 'x' ) ),
        'e2 NOT BETWEEN q{13} AND bv{34}::x'
    ],

    [ $e1->like('stuff'),          'e1 LIKE q{stuff}' ],
    [ $e1->like( _bval('stuff') ), 'e1 LIKE bv{stuff}::auto' ],
    [ $e1->like( _bval( 'stuff', 'x' ) ), 'e1 LIKE bv{stuff}::x' ],
    [ $e2->like('stuff'),          'e2 LIKE q{stuff}' ],
    [ $e2->like( _bval('stuff') ), 'e2 LIKE bv{stuff}::e2type' ],
    [ $e2->like( _bval( 'stuff', 'x' ) ), 'e2 LIKE bv{stuff}::x' ],

    [ $e1->as('junk'), 'e1 AS junk' ],

    [ !$e1->is_null,       'NOT e1 IS NULL' ],
    [ !$e1->like('stuff'), 'NOT e1 LIKE q{stuff}' ],

    [ ( $e1 == $e2 ) . AND . ( $e1 == $e2 ), '(e1 = e2) AND (e1 = e2)' ],
    [ ( $e1 == $e2 ) . OR .  ( $e1 == $e2 ), '(e1 = e2) OR (e1 = e2)' ],
    [ ( $e1 == $e2 ) . AND . !( $e1 == $e2 ), '(e1 = e2) AND NOT (e1 = e2)' ],
    [ ( $e1 == $e2 ) . OR . !( $e1 == $e2 ),  '(e1 = e2) OR NOT (e1 = e2)' ],
    [ !( $e1 == $e2 ) . AND . ( $e1 == $e2 ), 'NOT (e1 = e2) AND (e1 = e2)' ],
    [ !( $e1 == $e2 ) . OR .  ( $e1 == $e2 ), 'NOT (e1 = e2) OR (e1 = e2)' ],
    [
        !( $e1 == $e2 ) . AND . !( $e1 == $e2 ),
        'NOT (e1 = e2) AND NOT (e1 = e2)'
    ],
    [
        !( $e1 == $e2 ) . OR . !( $e1 == $e2 ), 'NOT (e1 = e2) OR NOT (e1 = e2)'
    ],
    [
        !( !( $e1 == $e2 ) . OR . !( $e1 == $e2 ) ),
        'NOT (NOT (e1 = e2) OR NOT (e1 = e2))'
    ],
    [
          ( ( $e1 == $e2 ) . OR . !( $e1 == $e2 ) ) 
        . AND
          . !( !( $e1 == $e2 ) . OR . !( $e1 == $e2 ) ),
        '((e1 = e2) OR NOT (e1 = e2)) AND NOT (NOT (e1 = e2) OR NOT (e1 = e2))'
    ],

#    [$e1 == ($e1 .OR. ($e2 .AND. ($e1 == $e2))), 'e1 = e1 OR (e2 AND e1 = e2)'],

    [
        ( ( $e1 == $e2 ) . AND . ( $e1 == $e2 ) )->as('junk'),
        '((e1 = e2) AND (e1 = e2)) AS junk'
    ],
    [
        $e1->between( 3, 34 ) . AND . $ce3,
        'e1 BETWEEN q{3} AND q{34} AND ((e1 AND e2) AND (e1 OR e2))'
    ],
    [ !( $e1 . AND . $e2 ), 'NOT (e1 AND e2)' ],
    [
        ( $e1 . AND . $e2 ) . OR . ( $e1 . AND . $e2 ),
        '(e1 AND e2) OR (e1 AND e2)'
    ],

    #    [ $e1 .= ' extra' ],
    #    [ $extra .= $e1, ' extra e1 extra' ],
  )
{

    #use Data::Dumper; $Data::Dumper::Indent = 1; $Data::Dumper::Maxdepth=2;
    #warn Dumper($_->[0]);

    eq_or_diff( $_->[0]->_as_string, $_->[1], $_->[1] );
}

done_testing();

