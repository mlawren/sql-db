use strict;
use warnings;
use Test::More tests => 52;
use Test::Memory::Cycle;

use DBI qw(SQL_BLOB);

require_ok('t/Schema.pm');
use_ok('SQL::DB::Schema');
use_ok('SQL::DB::Row');
can_ok('SQL::DB::Row', qw/
    make_class_from
/);


SQL::DB::Schema->import('define_tables');
define_tables(Schema->All);
my $schema = SQL::DB::Schema->new(qw/artists cds defaults/);

my $class = SQL::DB::Row->make_class_from($schema->table('artists')->columns);
is($class, 'SQL::DB::Row::artists.id_artists.name', 'class name');

can_ok('SQL::DB::Row::artists.id_artists.name', qw/
    new
    new_from_arrayref
    id
    set_id
    name
    set_name
    q_update
/);

my $new = $class->new_from_arrayref([qw(1 Homer)]);
isa_ok($new, 'SQL::DB::Row::artists.id_artists.name');

is($new->id, 1, 'id');
is($new->name, 'Homer', 'name');

memory_cycle_ok($new, 'memory cycle');

use Data::Dumper;
$Data::Dumper::Indent=1;
#die Dumper($new->q_insert);

my ($arows, @inserts) = $new->q_insert;
foreach my $insert (@inserts) {
    memory_cycle_ok($insert, 'memory cycle');

    my $q = $schema->query(@{$insert});
    isa_ok($q, 'SQL::DB::Schema::Query', 'query insert');
    is($q, 'INSERT INTO
    artists (id, name)
VALUES
    (?, ?)
', 'INSERT');
    memory_cycle_ok($q, 'memory cycle');
}


$new->set_id(2);
is($new->id, 2, 'id');
is($new->name, 'Homer', 'name');

my @updates;
($arows, @updates) = $new->q_update;
foreach my $update (@updates) {
    my $q = $schema->query(@{$update});
    isa_ok($q, 'SQL::DB::Schema::Query', 'query update');
    is($q, 'UPDATE
    artists
SET
    id = ?
WHERE
    id = ?
', 'INSERT');
    memory_cycle_ok($q, 'memory cycle');
}

my $new2 = $class->new({
    id => 2,
    name => 'Sideshow Bob',
});
is($new2->id, 2, 'id');
is($new2->name, 'Sideshow Bob', 'name');
$new2->set_id(3);
is($new2->id, 3, 'id');
is($new2->name, 'Sideshow Bob', 'name');
memory_cycle_ok($new2, 'memory cycle');


my $new3 = $class->new(
    id => 3,
    name => 'Skinner',
);
is($new3->id, 3, 'id');
is($new3->name, 'Skinner', 'name');
$new3->set_id(4);
is($new3->id, 4, 'id');
is($new3->name, 'Skinner', 'name');
memory_cycle_ok($new3, 'memory cycle');

my $dclass = SQL::DB::Row->make_class_from($schema->table('defaults')->columns);
is($dclass, 'SQL::DB::Row::defaults.id_defaults.scalar_defaults.sub_defaults.binary', 'default class name');

my $def = $dclass->new;
isa_ok($def, 'SQL::DB::Row::defaults.id_defaults.scalar_defaults.sub_defaults.binary');
memory_cycle_ok($def, 'memory cycle');
is($def->scalar, 1, 'scalar default');
is($def->sub, 2, 'sub default');

$def = Default->new;
isa_ok($def, 'Default', 'Default class');
memory_cycle_ok($def, 'memory cycle');
is($def->scalar, 1, 'scalar default');
is($def->sub, 2, 'sub default');

#is($schema->table('defaults')->column(0
my $adrow = $schema->arow('defaults');
is($adrow->binary->_column->bind_type, SQL_BLOB, 'binary is SQL_BLOB');

$class = SQL::DB::Row->make_class_from(
    $schema->table('artists')->columns,
    $schema->table('cds')->arow->id->as('id2'),
    $schema->table('cds')->column('title'),
    $schema->table('cds')->arow->year->as('year2'),
);
is($class, 'SQL::DB::Row::artists.id_artists.name_cds.id_cds.title_cds.year', 'class name');

$new = $class->new_from_arrayref([qw(1 Homer 2 Singing)]);
isa_ok($new, 'SQL::DB::Row::artists.id_artists.name_cds.id_cds.title_cds.year');
memory_cycle_ok($new, 'memory cycle');

can_ok($new, qw/
    new
    new_from_arrayref
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
($arows,@updates) =  $new->q_update;
foreach my $update (@updates) {
    my $q = $schema->query(@{$update});
    isa_ok($q, 'SQL::DB::Schema::Query', $q->_as_string);
    memory_cycle_ok($q, 'memory cycle');
}


