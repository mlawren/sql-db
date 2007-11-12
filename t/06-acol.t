use strict;
use warnings;
use Test::More tests => 52;
use Test::Memory::Cycle;

use_ok('SQL::DB::Schema::AColumn');
can_ok('SQL::DB::Schema::AColumn', qw/
    new
    as
    is_null
    is_not_null
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

my $acol = SQL::DB::Schema::AColumn->new($col, $arow);
isa_ok($acol, 'SQL::DB::Schema::AColumn');
memory_cycle_ok($acol, 'AColumn memory cycle');

foreach my $t (
    [$acol, 't01.fakecol' ],
    [$acol->as('fakecolas'), 't01.fakecol AS fakecolas' ],
    [$acol->is_null(), 't01.fakecol IS NULL' ],
    [!$acol, 't01.fakecol IS NULL' ],
    [$acol->is_not_null(), 't01.fakecol IS NOT NULL' ],
    [$acol->like('%str%'), 't01.fakecol LIKE ?' ],
    [$acol->asc(), 't01.fakecol ASC' ],
    [$acol->desc(), 't01.fakecol DESC' ],
    [$acol->set('val'), 'fakecol = ?' ],

    ){
    isa_ok($t->[0], 'SQL::DB::Schema::AColumn');
    isa_ok($t->[0]->_column, 'FakeCol');
    is($t->[0]->_arow, $arow, 'ARow');
    is($t->[0], $t->[1], $t->[1]);
    memory_cycle_ok($t->[0], 'memory cycle');
}

is_deeply([$acol->like('%str%')->bind_values], ['%str%'], 'LIKE bind_values');
is_deeply([$acol->set('val')->bind_values], ['val'], 'SET bind_values');

memory_cycle_ok($acol, 'final memory cycle');
