use strict;
use warnings;
use Test::More tests => 131;
use Test::Differences qw/eq_or_diff/;
use SQL::DB::Expr qw/AND OR _expr_join _bexpr_join /;

can_ok(
    'SQL::DB::Expr', qw/
      new
      _bvalues
      _btypes
      _multi
      _op
      _bexpr
      _expr_join
      _bexpr_join
      _expr_binary
      /
);

# Of course one should never do this, because Expr is kind of
# an abstract class. But for testing...
my $e1 = SQL::DB::Expr->new( _txt => 'e1' );
my $e2 = SQL::DB::Expr->new( _txt => 'e2', _btype => { default => 100 } );
my $ce1 = ( $e1 . AND . $e2 );
my $ce2 = ( $e1 . OR . $e2 );
my $ce3 = ( $ce1 . AND . $ce2 );
my $ce4 = ( ( $e1 != 6 ) . OR . ( $e2 < 3 ) );

my $alias = SQL::DB::Expr->new( _alias => 'alias', _txt => 'junk' );

is( $e1->_as_string, 'e1', 'e1 is e1' );

# Just the basic operators

foreach (
    [ _expr_join( ',', qw/one two three/ ), 'one,two,three', [], [] ],
    [
        _bexpr_join( ',', qw/one two three/ ), '?,?,?',
        [qw/one two three/], [ undef, undef, undef ]
    ],
    [ $alias, 'alias AS alias0', [], [] ],
    [ !$e1,   'NOT e1',          [], [] ],
    [ $e2 == 1, 'e2 = ?', [1], [ { default => 100 } ] ],
    [ $e1 != 1, 'e1 != ?', [1] ],
    [ $e1 < 1, 'e1 < ?', [1] ],
    [ $e1 > 1, 'e1 > ?', [1] ],
    [ $e1 >= 1, 'e1 >= ?', [1] ],
    [ $e1 <= 1, 'e1 <= ?', [1] ],
    [ 1 > $e2, '? > e2', [1], [ { default => 100 } ] ],
    [ 1 < $e1, '? < e1', [1] ],
    [ 1 >= $e1, '? >= e1', [1], [undef] ],
    [ 1 <= $e2, '? <= e2', [1], [ { default => 100 } ] ],
    [ $e1 + 1, 'e1 + ?', [1], [undef] ],
    [ $e1 - 1, 'e1 - ?', [1] ],
    [ 1 - $e1, '? - e1', [1] ],
    [ $e1 * 1, 'e1 * ?', [1] ],
    [ $e1 / 1, 'e1 / ?', [1] ],
    [ 1 / $e1, '? / e1', [1] ],
    [ $e1 == $e2, 'e1 = e2', [], [] ],
    [ $e1 == $e2, 'e1 = e2' ],
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
    [
        $e1->in( 9, 10, 11 ),
        'e1 IN (?, ?, ?)',
        [ 9,     10,    11 ],
        [ undef, undef, undef ]
    ],
    [
        $e1->not_in( 12, 13, 14 ),
        'e1 NOT IN (?, ?, ?)',
        [ 12,    13,    14 ],
        [ undef, undef, undef ]
    ],
    [
        $e2->in( 9, 10, 11 ),
        'e2 IN (?, ?, ?)',
        [ 9, 10, 11 ],
        [ { default => 100 }, { default => 100 }, { default => 100 } ]
    ],
    [
        $e2->not_in( 12, 13, 14 ),
        'e2 NOT IN (?, ?, ?)',
        [ 12, 13, 14 ],
        [ { default => 100 }, { default => 100 }, { default => 100 } ]
    ],
    [ $e1->between( $e1, $e2 ), 'e1 BETWEEN e1 AND e2', [], [] ],
    [ $e1->not_between( $e1, $e2 ), 'e1 NOT BETWEEN e1 AND e2' ],
    [
        $e1->between( 13, 34 ),
        'e1 BETWEEN ? AND ?',
        [ 13,    34 ],
        [ undef, undef ]
    ],
    [
        $e2->between( 13, 34 ),
        'e2 BETWEEN ? AND ?',
        [ 13, 34 ],
        [ { default => 100 }, { default => 100 } ]
    ],
    [
        $e1->not_between( 13, 34 ),
        'e1 NOT BETWEEN ? AND ?',
        [ 13,    34 ],
        [ undef, undef ]
    ],
    [
        $e2->not_between( 13, 34 ),
        'e2 NOT BETWEEN ? AND ?',
        [ 13, 34 ],
        [ { default => 100 }, { default => 100 } ]
    ],
    [ $e1->like('stuff'), 'e1 LIKE ?', ['stuff'], [undef] ],
    [ $e2->like('stuff'), 'e2 LIKE ?', ['stuff'], [ { default => 100 } ] ],
    [ $e1->as('junk'), 'e1 AS junk' ],
    [ ( $e1 == $e2 ) . AND . ( $e1 == $e2 ), '(e1 = e2) AND (e1 = e2)' ],
    [
        ( ( $e1 == $e2 ) . AND . ( $e1 == $e2 ) )->as('junk'),
        '((e1 = e2) AND (e1 = e2)) AS junk'
    ],
    [ ( $e1 == $e2 ) . OR . ( $e1 == $e2 ), '(e1 = e2) OR (e1 = e2)' ],
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
        $e1->between( 3, 34 ) . AND . $ce3,
        '(e1 BETWEEN ? AND ?) AND ((e1 AND e2) AND (e1 OR e2))',
        [ 3, 34 ]
    ],
    [ !$ce1,            'NOT (e1 AND e2)' ],
    [ $e1 . AND . $ce2, 'e1 AND (e1 OR e2)' ],
    [
        ( $e1 . AND . $e2 ) . OR . ( $e1 . AND . $e2 ),
        '(e1 AND e2) OR (e1 AND e2)'
    ],
    [ $ce1 . AND . $ce2,  '(e1 AND e2) AND (e1 OR e2)' ],
    [ $ce1 . AND . !$ce2, '(e1 AND e2) AND NOT (e1 OR e2)' ],
    [ $ce2 . AND . $ce3,  '(e1 OR e2) AND ((e1 AND e2) AND (e1 OR e2))' ],
    [
        !( $ce2 . AND . $ce3 ),
        'NOT ((e1 OR e2) AND ((e1 AND e2) AND (e1 OR e2)))'
    ],
    [ $ce2 . AND . !$ce3, '(e1 OR e2) AND NOT ((e1 AND e2) AND (e1 OR e2))' ],
    [ $ce4, '(e1 != ?) OR (e2 < ?)', [ 6, 3 ] ],
    [ $e2->not_in( 3, 4, 5 ), 'e2 NOT IN (?, ?, ?)', [ 3, 4, 5 ] ],
    [
        $ce4 . AND . $e2->not_in( 3, 4, 5 ),
        '((e1 != ?) OR (e2 < ?)) AND e2 NOT IN (?, ?, ?)',
        [ 6, 3, 3, 4, 5 ]
    ],

  )
{

    #use Data::Dumper; $Data::Dumper::Indent = 1; $Data::Dumper::Maxdepth=2;
    #warn Dumper($_->[0]);

    eq_or_diff( $_->[0]->_as_string, $_->[1], $_->[1] );
    if ( @$_ > 2 ) {
        is_deeply( $_->[0]->_bvalues, $_->[2],
            '_bvalues '
              . join( ',', map { defined $_ ? $_ : 'undef' } @{ $_->[2] } ) );
    }
    if ( @$_ > 3 ) {
        is_deeply( $_->[0]->_btypes, $_->[3],
            '_btypes '
              . join( ',', map { defined $_ ? $_ : 'undef' } @{ $_->[3] } ) );
    }
}

done_testing();

