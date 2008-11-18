use strict;
use warnings;
use Test::More tests => 75;
use Test::Memory::Cycle;

use DBI qw(SQL_BLOB);

require_ok('t/TestLib.pm');
use_ok('SQL::DB::Schema', 'define_tables');
use_ok('SQL::DB::Row');
can_ok('SQL::DB::Row', qw/
    make_class_from
/);


define_tables(TestLib->All);

my $schema = SQL::DB::Schema->new(qw/artists cds defaults/);

my $class = SQL::DB::Row->make_class_from($schema->table('artists')->columns);
is($class, 'SQL::DB::Row::artists.id_artists.name_artists.ucname', 'class name');

can_ok('SQL::DB::Row::artists.id_artists.name_artists.ucname', qw/
    new
    new_from_arrayref
    id
    set_id
    name
    ucname
    set_name
    q_insert
    q_update
    q_delete
    quickdump
    _column_names
    _hashref
    _modified
    _hashref_modified
/);

my $new = $class->new_from_arrayref([qw(1 Homer)]);
isa_ok($new, 'SQL::DB::Row::artists.id_artists.name_artists.ucname');

is_deeply([$new->_column_names], [qw/id name ucname/], '_column_names');

is($new->id, 1, 'id');

is_deeply($new->_hashref, {name => 'Homer', id => 1, ucname => undef}, 'hashref');

is_deeply($new->_hashref_modified, {}, 'hashref modified');

ok(!$new->_modified('id'), 'not modified');

is($new->name, 'Homer', 'name');

is($new->ucname, undef, 'ucname');

ok(!$new->_modified('name'), 'not modified');

is($new->quickdump, 'id           = 1
name         = Homer
ucname       = NULL
', 'dump ok');

$new->set_name('Homer');

is_deeply($new->_hashref, {name => 'Homer', id => 1, ucname => 'HOMER'}, 'hashref');

is_deeply($new->_hashref_modified, {name => 'Homer', ucname => 'HOMER'}, 'hashref modified');

is($new->quickdump, 'id           = 1
name[m]      = Homer
ucname[m]    = HOMER
', 'dump ok');


memory_cycle_ok($new, 'memory cycle');

$new = $class->new_from_arrayref([qw(1 Homer)]);

use Data::Dumper;
$Data::Dumper::Indent=1;
#die Dumper($new->q_insert);

my ($arows, @inserts) = $new->q_insert;
foreach my $insert (@inserts) {
    memory_cycle_ok($insert, 'memory cycle');

    my $q = $schema->query(@{$insert});
    isa_ok($q, 'SQL::DB::Schema::Query', 'query insert');
    is($q, 'INSERT INTO
    artists (id, name, ucname)
VALUES
    (?, ?, ?)
', 'INSERT');
    memory_cycle_ok($q, 'memory cycle');
}


$new->set_id(2);
is($new->id, 2, 'id');
ok($new->_modified('id'), 'modified');
is($new->name, 'Homer', 'name');
ok(!$new->_modified('name'), 'not modified');

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

($arows, @updates) = $new->q_delete;
foreach my $update (@updates) {
    my $q = $schema->query(@{$update});
    isa_ok($q, 'SQL::DB::Schema::Query', 'query delete');
    is($q, 'DELETE FROM
    artists
WHERE
    id = ?
', 'DELETE');
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
is($adrow->binary->_column->bind_type, undef, 'bind type undef');

$class = SQL::DB::Row->make_class_from(
    $schema->table('artists')->columns,
    $schema->table('cds')->arow->id->as('id2'),
    $schema->table('cds')->column('title'),
    $schema->table('cds')->arow->year->as('year2'),
);
is($class, 'SQL::DB::Row::artists.id_artists.name_artists.ucname_cds.id_cds.title_cds.year', 'class name');

$new = $class->new_from_arrayref([qw(1 Homer HOMER 2 Singing)]);
isa_ok($new, 'SQL::DB::Row::artists.id_artists.name_artists.ucname_cds.id_cds.title_cds.year');
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

my $acol = $schema->acol('id');
$class = SQL::DB::Row->make_class_from($acol);
is($class, 'SQL::DB::Row::id', 'class from abstract column');

# Checking the multiple arow update service when different arows from
# the same table are used
my $artists = $schema->arow('artists');
my $artists2 = $schema->arow('artists');

$class = SQL::DB::Row->make_class_from(
        $artists->id,
        $artists->name,
        $artists->ucname,
        $artists2->id->as('id2'),
        $artists2->name->as('name2'),
);

my $n = $class->new_from_arrayref([]);
($arows,@updates) = $n->q_update;
ok(!@updates, 'no updates with no changes');

$n->set_id(1);
is_deeply($n->_hashref_modified, {
    id        => 1,
}, 'hashref ok');

($arows,@updates) = $n->q_update;
is(scalar @updates, 1, 'one change one update');

my $query = $schema->query(@{$updates[0]});
is($query->as_string, 'UPDATE
    artists
SET
    id = ?
WHERE
    id = ?
', 'query ok');

$n->set_name2(1);
is_deeply($n->_hashref_modified, {
    id        => 1,
    name2     => 1,
    ucname    => 1,
}, 'hashref ok');

is_deeply($n->_hashref, {
    id        => 1,
    id2       => undef,
    name      => undef,
    name2     => 1,
    ucname    => 1,
}, 'hashref ok');

($arows,@updates) = $n->q_update;
is(scalar @updates, 1, 'no second primary key, one update');

$query = $schema->query(@{$updates[0]});
is($query->as_string, 'UPDATE
    artists
SET
    id = ?, ucname = ?
WHERE
    id = ?
', 'query ok');

$n->set_id2(1);
is_deeply($n->_hashref_modified, {
    id        => 1,
    id2       => 1,
    name2     => 1,
    ucname    => 1,
}, 'hashref ok');

($arows,@updates) = $n->q_update;
is(scalar @updates, 2, 'second primary key, two updates');

$n->set_name(1);
($arows,@updates) = $n->q_update;
is(scalar @updates, 2, 'second primary key, two updates');

# Not convinced the following tests are good. Who is to say that the
# order of these two queries is always going to be the same?

if (0) {
$query = $schema->query(@{$updates[0]});
is($query->as_string, 'UPDATE
    artists
SET
    id = ?, name = ?
WHERE
    id = ?
', 'query ok');

$query = $schema->query(@{$updates[1]});
is($query->as_string, 'UPDATE
    artists
SET
    id = ?, name = ?, ucname = ?
WHERE
    id = ?
', 'query ok');


is($n->quickdump, 'id[m]        = 1
name[m]      = 1
ucname[m]    = 1
id2[m]       = 1
name2[m]     = 1
', 'dump ok');

is_deeply($n->_hashref, {
    id        => 1,
    name      => 1,
    ucname    => 1,
    id2       => 1,
    name2     => 1,
}, 'hashref ok');

}
