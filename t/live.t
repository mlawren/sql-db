#!/usr/bin/perl
use strict;
use warnings;
use lib 't/lib';
use Test::More tests => 4;
use SQL::DB qw(define_tables max min count coalesce sum);
use SQLDBTest;

require_ok('t/TestLib.pm');

#$SQL::DB::DEBUG = 3;
#$SQL::DB::Schema::DEBUG=1;
#$SQL::DB::Schema::ARow::DEBUG = 3;
#$SQL::DB::Schema::Query::DEBUG = 1;

define_tables(TestLib->All);

my $db = SQLDBTest->new;
$db->test_connect();
$db->test_populate();

my $track   = $db->arow('tracks');
my $cd     = $db->arow('cds');
my $artist = $db->arow('artists');

#$db->do(
#    delete_from => $track,
#);
#
#$db->do_nopc(
#    delete_from => $cd,
#);
#
#$db->do(
#    delete_from => $artist,
#        where => $arow->id == 4,
#);

my $f = Fan->new;
is($f->craziness, 1, 'Default scalar');
is($f->subcraziness, 2, 'Default scalar');



my @objs = $db->fetch(
    select   => [ $track->id,$track->title,
                  $cd->year,$artist->name ],
    from     => [$track, $cd, $artist],
    distinct => 1,
    where    => ( $track->length < 248 ) &
              ! ($cd->year > 1997), #&
#                  $track->title->like('%Life%'),
    order_by => [ $track->title ],
#    limit => '2',
);

#  print $query,"\n";

foreach my $obj (@objs) {
    print $obj->id,', ',$obj->title,', ',$obj->name,"\n";
}

#warn $db->quickrows(@objs);


@objs = $db->fetch(
    select   => [ count($track->id)->as('count_id'),
                   $cd->title,
                   max($track->length)->as('max_length'),
                   sum($track->length)->as('sum_length')],
    from       => [$track],
    inner_join => $cd,
    on         => $track->cd == $cd->id,
    group_by   => [$cd->title],
#    where     =># ( $track->length < 248 ) &
#                 ! ($track->cd->year > 1997),
);

#  print $query,"\n";

foreach my $obj (@objs) {
    print 'Title: '. $obj->title ."\n";
    print '# trackss: '. $obj->count_id ."\n";
    print 'Longest tracks: ' . $obj->max_length ."\n";
    print 'CD Length: ' . $obj->sum_length ."\n\n";
}



$db->do(
    update => $cd->set_year(2006),
    where    => $cd->id == 10,
);

my $track2 = $db->arow('tracks');
my $cd2 = $db->arow('cds');

my $q2 =  $db->query(
    select   => [ $track2->title, $cd2->year ],
    distinct => 1,
    from     => [$track2, $cd2],
    where    => ( $track2->length < 248 ) & ! ($cd2->year > 1997),
);


@objs = $db->fetch(
    select   => [ $track->title, $cd->year],
    from     => [$track, $cd],
    distinct => 1,
    where    => ( $track->length > 248 ) & ! ($cd->year < 1997),
    union    => $q2,
);

my @objs2 = $db->fetch_nopc(
    select   => [ $track->title, $cd->year],
    from     => [$track, $cd],
    distinct => 1,
    where    => ( $track->length > 248 ) & ! ($cd->year < 1997),
    union    => $q2,
);

is_deeply(\@objs, \@objs2, 'fetch and fetch_nopc');

my $fan = $db->arow('fans');
my $link = $db->arow('artists_fans');

my $cursor = $db->fetch(
    select => [$fan->name, $fan->craziness],
    from   => [$fan, $artist, $link],
    where   => ($artist->name == 'Queen') &
                ($link->fan == $fan->id) & ($link->artist == $artist->id)
);

print "Queen fanss (with craziness)\n";
while (my $next = $cursor->next) {
    print $next->name .' ('.$next->craziness .")\n";
}

my $res = $db->fetch1(
    select => [$fan->name, $fan->craziness],
    from   => [$fan, $artist, $link],
    where   => ($artist->name == 'Queen') &
                ($link->fan == $fan->id) & ($link->artist == $artist->id)
);

print "Only the first Queen fans (with craziness)\n";
print $res->name .' ('.$res->craziness .")\n";


$cursor = $db->fetch(
    select => [$fan->name, $fan->craziness],
    from   => $fan,
    where   => $fan->id->not_in($db->query(select => [$link->fan],
                    from => [$link])),
);

print "Un-fanss\n";
while (my $next = $cursor->next) {
    print $next->name .' ('.$next->craziness .")\n";
}


my @res = $db->fetch(
    select => [$cd->title],
    from   => $cd,
    left_join => $artist,
    on       => $cd->artist == $artist->id,
    where   => $artist->name == 'Queen',
);

foreach (@res) {
    print "Queen Album: ". $_->title ."\n";
}

@res = $db->fetch(
    select => [$cd->title, $cd->id],
    from    => [$cd],
    left_join => $artist,
    on      => $artist->id == $cd->artist,
    where   => $artist->name->like('Queen'),
);

foreach (@res) {
    print "Queen Album: ". $_->title ."\n";
}

$db->do(
    delete_from => $track,
    where       => $track->id == 3,
);

$db->do(
    insert_into => [$track->id, $track->cd, $track->title, $track->length],
    values      => [3, 2, 'Who wants to live forever?', 285]
);

#warn ref($track->length($track->length + 1));

$db->do(
    update => [ $track->set_id(3),
                $track->set_cd(2),
                $track->set_title('Who wants to live forever?'),
                $track->set_length($track->length + 1)
    ],
    where => $track->id == 3,
);


