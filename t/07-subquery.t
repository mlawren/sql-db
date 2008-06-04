use strict;
use warnings;
use Test::More tests => 5;
use Test::Memory::Cycle;

require 't/TestLib.pm';

use SQL::DB::Schema qw(define_tables);

define_tables(TestLib->All);
my $s = SQL::DB::Schema->new(qw/artists/);

my $artist = $s->arow('artists');

my $q;


$q = $s->query(
    select => [$artist->id],
    from => $artist,
    where => $artist->id == 1,
    limit => 1,
);
is($q, 'SELECT
    artists1.id
FROM
    artists AS artists1
WHERE
    artists1.id = ?
LIMIT 1
', 'select');
memory_cycle_ok($q, 'memory cycle');

my $a2 = $s->arow('artists');

my $q2 = $s->query(
    select => [$a2->name],
    from   => $a2,
    where  => $a2->id->not_in($q),
);

is($q2, 'SELECT
    artists2.name
FROM
    artists AS artists2
WHERE
    artists2.id NOT IN (SELECT
    artists1.id
FROM
    artists AS artists1
WHERE
    artists1.id = ?
LIMIT 1
)
', 'subselect');

memory_cycle_ok($q2, 'memory cycle');

$q2 = $s->query(
    select => [$a2->name],
    from   => $a2,
    where  => $a2->id == $q,
);

is($q2, 'SELECT
    artists2.name
FROM
    artists AS artists2
WHERE
    artists2.id = (SELECT
    artists1.id
FROM
    artists AS artists1
WHERE
    artists1.id = ?
LIMIT 1
)
', 'subselect');

