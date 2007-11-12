use strict;
use warnings;
use Test::More tests => 10;

use_ok('SQL::DB', qw(define_tables max min count coalesce sum));
require_ok('t/TestLib.pm');

#$SQL::DB::DEBUG = 3;
#$SQL::DB::Schema::DEBUG=1;
#$SQL::DB::Schema::ARow::DEBUG = 3;
#$SQL::DB::Schema::Query::DEBUG = 1;

define_tables(TestLib->All);

my $db = SQL::DB->new;
isa_ok($db, 'SQL::DB');


$db->connect(TestLib->dbi);
ok($db->deploy, 'deploy');

ok($db->create_seq('test'), "Sequence test created");
my $val;
ok($val = $db->seq('test'), "Sequence test $val");
ok($val = $db->seq('test'), "Sequence test $val");
ok($val = $db->seq('test'), "Sequence test $val");

my $track   = $db->arow('tracks');
my $cd     = $db->arow('cds');
my $artist = $db->arow('artists');

$db->do(
    delete_from => $track,
);

$db->do(
    delete_from => $cd,
);

$db->do(
    delete_from => $artist,
#        where => $arow->id == 4,
);

my $f = Fan->new;
is($f->craziness, 1, 'Default scalar');
is($f->subcraziness, 2, 'Default scalar');


while (my $str = <DATA>) {
    chomp($str);
    my @values = split(',',$str);

    my $class = shift @values;
    my $rem = $db->arow($class);

    my $q = $db->do(
            insert_into => [$rem->_columns],
            values  => [@values],
    );
}


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


__DATA__
artists,1,Queen
artists,2,INXS
cds,1,A Kind of Magic,1986,1
cds,2,A Night at the Opera,1978,2
tracks,1,2,Death on Two Legs (Dedicated to.......),223
tracks,2,2,Lazing On A Sunday Afternoon,67
tracks,3,2,I'm in Love with My Car,187
tracks,4,2,You're My Best Friend,170
tracks,5,2,39,211
tracks,6,2,Sweet Lady,243
tracks,7,2,Seaside Rendezvous,135
tracks,8,2,The Prophet's Song,501
tracks,9,2,Love of My Life,219
tracks,10,2,Good Company,203
tracks,11,2,Bohemian Rhapsody,355
tracks,12,2,God Save the Queen,138
tracks,13,2,I'm in Love with My Car,208
tracks,14,2,You're My Best Friend,172
tracks,15,2,One Vision,310
tracks,16,1,A Kind of Magic,264
tracks,17,1,One Year of Love,266
tracks,18,1,Pain Is So Close to Pleasure,261
tracks,19,1,Friends Will Be Friends,247
tracks,20,1,Who Wants to Live Forever,305
tracks,21,1,Gimme the Prize,274
tracks,22,1,Don't Lose Your Head,278
tracks,23,1,Princes of the Universe,212
fans,1,fans1,100,1
fans,2,fans2,83,1
fans,3,fans3,3,1
fans,4,Faker,4,1
fans,5,fans5,52,1
fans,6,fans6,88,1
fans,7,fans7,36,1
fans,8,Not a fans,0,1
artists_fans,1,1
artists_fans,1,2
artists_fans,1,3
artists_fans,1,6
artists_fans,1,7
artists_fans,2,1
artists_fans,2,3
artists_fans,2,5
artists_fans,2,6
