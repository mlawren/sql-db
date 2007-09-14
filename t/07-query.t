use strict;
use warnings;
use Test::More tests => 5;
require 't/Schema.pm';

use_ok('SQL::DB::Schema');
use_ok('SQL::DB::Query');
can_ok('SQL::DB::Query', qw/
    new
    st_where
    st_insert_into
    st_insert
    st_values
    st_update
    st_select
    st_selecto
    st_distinct
    st_for_update
    st_from
    st_on
    st_inner_join
    st_left_outer_join
    st_left_join
    st_right_outer_join
    st_right_join
    st_full_join
    st_full_outer_join
    st_cross_join
    st_union
    st_group_by
    st_order_by
    st_limit
    st_offset
    st_delete
    st_delete_from
/);

my $s = SQL::DB::Schema->new(Schema->All);

my $artist = Artist::Abstract->new();

my $q;

$q = $s->query(
    select => [$artist->id],
);
like($q, qr/^SELECT.*id/sm, $q);

$q = $s->query(
    update => [$artist->id->set(4)],
);
like($q, qr/^UPDATE.*SET.*id = /sm, $q);


__END__
my $col = FakeCol->new;
my $arow = 'Abstract Row';

my $acol = SQL::DB::AColumn->_new($col, $arow);
isa_ok($acol, 'SQL::DB::AColumn');

foreach my $t (
    [$acol, 'fakecol' ],
    [$acol->as('fakecolas'), 'fakecol AS fakecolas' ],
    [$acol->is_null(), 'fakecol IS NULL' ],
    [$acol->like('%str%'), 'fakecol LIKE ?' ],
    [$acol->asc(), 'fakecol ASC' ],
    [$acol->desc(), 'fakecol DESC' ],
    [$acol->set('val'), 'fakecol = ?' ],

    ){
    isa_ok($t->[0], 'SQL::DB::AColumn');
    isa_ok($t->[0]->_column, 'FakeCol');
    is($t->[0]->_arow, $arow, 'ARow');
    is($t->[0], $t->[1], $t->[1]);
}

is_deeply([$acol->like('%str%')->bind_values], ['%str%'], 'LIKE bind_values');
is_deeply([$acol->set('val')->bind_values], ['val'], 'SET bind_values');

