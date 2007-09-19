use strict;
use warnings;
use Test::More tests => 33;

use_ok('SQL::DB::AColumn');
can_ok('SQL::DB::AColumn', qw/
    new
    as
    is_null
    like
    asc
    desc
    set
/);


package FakeCol;
sub new {
    return bless({},'FakeCol');
}
sub name {
    return 'fakecol';
}

package FakeARow;
sub new {
    return bless({},'FakeARow');
}
sub _alias {
    return 't01';
}
sub _name {
    return 't01';
}

package main;


my $col = FakeCol->new;
my $arow = FakeARow->new;

my $acol = SQL::DB::AColumn->new($col, $arow);
isa_ok($acol, 'SQL::DB::AColumn');

foreach my $t (
    [$acol, 't01.fakecol' ],
    [$acol->as('fakecolas'), 't01.fakecol AS fakecolas' ],
    [$acol->is_null(), 't01.fakecol IS NULL' ],
    [$acol->like('%str%'), 't01.fakecol LIKE ?' ],
    [$acol->asc(), 't01.fakecol ASC' ],
    [$acol->desc(), 't01.fakecol DESC' ],
    [$acol->set('val'), 'fakecol = ?' ],

    ){
    isa_ok($t->[0], 'SQL::DB::AColumn');
    isa_ok($t->[0]->_column, 'FakeCol');
    is($t->[0]->_arow, $arow, 'ARow');
    is($t->[0], $t->[1], $t->[1]);
}

is_deeply([$acol->like('%str%')->bind_values], ['%str%'], 'LIKE bind_values');
is_deeply([$acol->set('val')->bind_values], ['val'], 'SET bind_values');

