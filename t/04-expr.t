use strict;
use warnings;
use Test::More;
use Test::Differences qw/eq_or_diff/;
use SQL::DB::Expr qw/AND OR _sql _quote _bval _expr_join /;

$| = 1;

my $sql = SQL::DB::Expr::SQL->new( val => 'sql' );
isa_ok $sql, 'SQL::DB::Expr::SQL';
is $sql,     'sql', $sql;
is $sql,     _sql('sql'), '_sql ' . $sql;

my $q1 = SQL::DB::Expr::Quote->new( val => 'val' );
isa_ok $q1, 'SQL::DB::Expr::Quote';
is $q1,     'q{val}', $q1;
is $q1,     _quote('val'), '_quote ' . $q1;
is _quote($sql), $sql, '_quote sql';
is _quote($q1),  $q1,  '_quote quote';

my $bv1 = SQL::DB::Expr::BindValue->new( val => 'val' );
isa_ok $bv1, 'SQL::DB::Expr::BindValue';
is $bv1,     'bv{val}::(none)', $bv1;
is $bv1,     _bval('val'), '_bval ' . $bv1;

my $bv2 = SQL::DB::Expr::BindValue->new( val => 'val', type => 'type' );
isa_ok $bv2, 'SQL::DB::Expr::BindValue';
is $bv2,     'bv{val}::type', $bv2;
is $bv2,     _bval( 'val', 'type' ), '_bval ' . $bv2;

is _bval($sql), $sql, '_bval sql';
is _bval($q1),  $q1,  '_bval quote';
is _bval($bv1), $bv1, '_bval bval';

# Of course one should never do this, because Expr is kind of
# an abstract class. But for testing...
my $e1 = SQL::DB::Expr->new( _txt => ['e1'] );
my $e2 = SQL::DB::Expr->new( _txt => ['e2'], _type => 'e2type' );
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
    [ $alias,             'alias AS alias0' ],
    [ !$e1,               'NOT e1' ],
    [ $e2 == 1,           'e2 = bv{1}::e2type' ],
    [ $e2 == 'x',         'e2 = bv{x}::e2type' ],
    [ $e1 != _quote(1),   'e1 != q{1}' ],
    [ $e1 != _quote('x'), 'e1 != q{x}' ],
    [ $e1 < 1,            'e1 < bv{1}::(none)' ],
    [ $e1 > 1,            'e1 > bv{1}::(none)' ],
    [ $e1 >= 1,           'e1 >= bv{1}::(none)' ],
    [ $e1 <= 1,           'e1 <= bv{1}::(none)' ],
    [ 1 > $e2,            'bv{1}::e2type > e2' ],
    [ 1 < $e1,            'bv{1}::(none) < e1' ],
    [ 1 >= $e1,           'bv{1}::(none) >= e1' ],
    [ 1 <= $e2,           'bv{1}::e2type <= e2' ],
    [ $e1 + 1,            'e1 + bv{1}::(none)' ],
    [ $e1 - 1,            'e1 - bv{1}::(none)' ],
    [ 1 - $e1,            'bv{1}::(none) - e1' ],
    [ $e1 * 1,            'e1 * bv{1}::(none)' ],
    [ $e1 / 1,            'e1 / bv{1}::(none)' ],
    [ 1 / $e1,            'bv{1}::(none) / e1' ],
    [ $e1 == $e2,         'e1 = e2' ],
    [ $e1 == $e2,         'e1 = e2' ],
    [ $e1 != $e2,         'e1 != e2' ],
    [ $e1 . AND . $e2,    'e1 AND e2' ],
    [ !$e1,               'NOT e1' ],
    [ $e1 . OR . $e2,     'e1 OR e2' ],
    [ $e1 < $e2,          'e1 < e2' ],
    [ $e1 > $e2,          'e1 > e2' ],
    [ $e1 >= $e2,         'e1 >= e2' ],
    [ $e1 <= $e2,         'e1 <= e2' ],
    [ $e1 + $e2,          'e1 + e2' ],
    [ $e1 - $e2,          'e1 - e2' ],
    [ $e1->is_null,       'e1 IS NULL' ],
    [ $e1->is_not_null,   'e1 IS NOT NULL' ],
    [ $e1->in( $e1, $e2 ), 'e1 IN (e1, e2)' ],
    [ $e1->not_in( $e1, $e2 ), 'e1 NOT IN (e1, e2)' ],
    [ $e1->in( 9,   10 ),  'e1 IN (bv{9}::(none), bv{10}::(none))' ],
    [ $e1->in( 'x', 'y' ), 'e1 IN (bv{x}::(none), bv{y}::(none))' ],
    [ $e2->in( 9,   10 ),  'e2 IN (bv{9}::e2type, bv{10}::e2type)' ],
    [ $e2->in( 'x', 'y' ), 'e2 IN (bv{x}::e2type, bv{y}::e2type)' ],
    [ $e1->not_in( 9,   10 ),  'e1 NOT IN (bv{9}::(none), bv{10}::(none))' ],
    [ $e2->not_in( 9,   10 ),  'e2 NOT IN (bv{9}::e2type, bv{10}::e2type)' ],
    [ $e2->not_in( 'x', 'y' ), 'e2 NOT IN (bv{x}::e2type, bv{y}::e2type)' ],
    [ $e1->between( $e1, $e2 ), 'e1 BETWEEN e1 AND e2' ],
    [ $e1->between( 13,  'x' ), 'e1 BETWEEN bv{13}::(none) AND bv{x}::(none)' ],
    [ $e1->not_between( $e1, $e2 ), 'e1 NOT BETWEEN e1 AND e2' ],
    [ $e2->between( $e1, $e2 ), 'e2 BETWEEN e1 AND e2' ],
    [ $e2->between( 13, 34 ), 'e2 BETWEEN bv{13}::e2type AND bv{34}::e2type' ],
    [ $e2->between( 'x', 'y' ), 'e2 BETWEEN bv{x}::e2type AND bv{y}::e2type' ],
    [ $e2->not_between( $e1, $e2 ), 'e2 NOT BETWEEN e1 AND e2' ],
    [
        $e2->not_between( 13, 34 ),
        'e2 NOT BETWEEN bv{13}::e2type AND bv{34}::e2type'
    ],
    [
        $e2->not_between( 'x', 'y' ),
        'e2 NOT BETWEEN bv{x}::e2type AND bv{y}::e2type'
    ],

    [ $e1->like('stuff'),           'e1 LIKE bv{stuff}::(none)' ],
    [ $e1->like( _sql('stuff') ),   'e1 LIKE stuff' ],
    [ $e1->like( _quote('stuff') ), 'e1 LIKE q{stuff}' ],
    [ $e1 . _sql(" LIKE 'x'"),      "e1 LIKE 'x'" ],

    [ $e2->like('stuff'), 'e2 LIKE bv{stuff}::e2type' ],
    [ $e1->as('junk'),    'e1 AS "junk"' ],

    [ !$e1->is_null,       'NOT e1 IS NULL' ],
    [ !$e1->like('stuff'), 'NOT e1 LIKE bv{stuff}::(none)' ],

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
        !( $e1 == $e2 ) . OR . !( $e1 == $e2 ),
        'NOT (e1 = e2) OR NOT (e1 = e2)'
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
        '((e1 = e2) AND (e1 = e2)) AS "junk"'
    ],
    [
        $e1->between( 'x', 34 ) . AND . $ce3,
'e1 BETWEEN bv{x}::(none) AND bv{34}::(none) AND ((e1 AND e2) AND (e1 OR e2))'
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

