use strict;
use warnings;
use Test::More tests => 22;

require_ok('t/Schema.pm');
use_ok('SQL::DB::Schema');
use_ok('SQL::DB::Row');
can_ok('SQL::DB::Row', qw/
    make_class_from
    new
/);


my $schema = SQL::DB::Schema->new(Schema->All);

my $class = SQL::DB::Row->make_class_from($schema->table('artists')->columns);
is($class, 'SQL::DB::Row::artists.id_artists.name', 'class name');

my $new = $class->new([qw(1 Homer)]);
isa_ok($new, 'SQL::DB::Row::artists.id_artists.name');

can_ok('SQL::DB::Row::artists.id_artists.name', qw/
    id
    set_id
    name
    set_name
/);

is($new->id, 1, 'id');
is($new->name, 'Homer', 'name');
$new->set_id(2);
is($new->id, 2, 'id');
is($new->name, 'Homer', 'name');

foreach my $update ($new->q_update) {
    my $q = $schema->query(@{$update});
    isa_ok($q, 'SQL::DB::Query', $q->_as_string);
}



$class = SQL::DB::Row->make_class_from(
    $schema->table('artists')->columns,
    $schema->table('cds')->arow->id->as('id2'),
    $schema->table('cds')->column('title'),
    $schema->table('cds')->arow->year->as('year2'),
);
is($class, 'SQL::DB::Row::artists.id_artists.name_cds.id_cds.title_cds.year', 'class name');

$new = $class->new([qw(1 Homer 2 Singing)]);
isa_ok($new, 'SQL::DB::Row::artists.id_artists.name_cds.id_cds.title_cds.year');

can_ok($new, qw/
    id
    name
    title
    id2
    year2
    set_id
    set_name
    set_title
    set_id2
    set_year2
/);

is($new->id, 1, 'id');
is($new->name, 'Homer', 'name');
is($new->title, 'Singing', 'title');
$new->set_name('Marge');
$new->set_title('In the rain');
$new->set_year2(2007);
is($new->name, 'Marge', 'name');
is($new->year2, 2007, 'title');

use Data::Dumper;
$Data::Dumper::Maxdepth = 3;
#warn Dumper($new->q_update);
foreach my $update ($new->q_update) {
    my $q = $schema->query(@{$update});
    isa_ok($q, 'SQL::DB::Query', $q->_as_string);
}


