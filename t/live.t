use strict;
use warnings;
use Test::More;

BEGIN {
    if (!eval {require DBD::SQLite;1;}) {
        plan skip_all => "DBD::SQLite not installed: $@";
    }
    else {
        plan tests => 7;
    }

}
END {
    unlink "/tmp/sqldb$$.db";
}

use_ok('SQL::DB');
require_ok('t/Schema.pm');

#$SQL::DB::DEBUG = 3;
#$SQL::DB::ARow::DEBUG = 3;
#$SQL::DB::Query::DEBUG = 1;


our $schema;
our $db = SQL::DB->new;
isa_ok($db, 'SQL::DB');

$db = SQL::DB->new(Schema->All);
isa_ok($db, 'SQL::DB');

#our $schema = $db->schema(Schema->All);
isa_ok($db->schema, 'SQL::DB::Schema');

$db->connect(
    "dbi:SQLite:/tmp/sqldb$$.db",undef,undef,
#    'dbi:Pg:dbname=test;port=5433', 'rekudos', 'rekudos',
    {PrintError => 0, RaiseError => 1},
);
ok(1, 'connected');

$db->deploy;
ok(1, 'deployed');

my $arow = Track->arow;
$db->do(
    delete_from => $arow,
);

$arow = CD->arow;
$db->do(
    delete_from => $arow,
);

$arow = Artist->arow;
$db->do(
    delete_from => $arow,
#        where => $arow->id == 4,
);

while (my $str = <DATA>) {
    chomp($str);
    my @values = split(',',$str);

    my $class = shift @values;
    my $rem = $class->arow;

    my $q = $db->do(
            insert_into => [$rem->_columns],
            values  => [@values],
    );
}

my $track = Track->arow;
my $cd = CD->arow;
my $artist = Artist->arow;

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


$track = Track->arow;
$cd = CD->arow;
@objs = $db->fetch(
    select   => [ $track->id->func('count'),
                   $cd->title,
                   $track->length->func('max'),
                   $track->length->func('sum')],
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
    print '# Tracks: '. $obj->count_id ."\n";
    print 'Longest Track: ' . $obj->max_length ."\n";
    print 'CD Length: ' . $obj->sum_length ."\n\n";
}



$cd = CD->arow;
$db->do(
    update => $cd,
    set     => [$cd->year->set(2006)],
    where    => $cd->id == 10,
);

$cd = CD->arow;
my $track2 = Track->arow;
my $cd2 = CD->arow;
my $q2 =  $db->schema->query(
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


my $fan = Fan->arow;
my $link = ArtistFan->arow;
my @res = $db->fetch(
    select => [$fan->name, $fan->craziness],
    from   => [$fan, $artist, $link],
    where   => ($artist->name == 'Queen') &
                ($link->fan == $fan->id) & ($link->artist == $artist->id)
);

print "Queen Fans (with craziness)\n";
foreach (@res) {
    print $_->name .' ('.$_->craziness .")\n";
}


$fan = Fan->arow;
$link = ArtistFan->arow;
@res = $db->fetch(
    select => [$fan->name, $fan->craziness],
    from   => $fan,
    where   => $fan->id->not_in($db->schema->query(select => [$link->fan],
                    from => [$link])),
);

print "Un-Fans\n";
foreach (@res) {
    print $_->name .' ('.$_->craziness .")\n";
}


my $a = Artist->arow;
my $c = CD->arow;
@res = $db->fetch(
    select => [$cd->title],
    from   => $cd,
    left_join => $a,
    on       => $cd->artist == $a->id,
    where   => $a->name == 'Queen',
);

foreach (@res) {
    print "Queen Album: ". $_->title ."\n";
}

$c = CD->arow;
$a = Artist->arow;
@res = $db->fetch(
    selecto => [$c->title, $c->id],
    from    => [$c],
    left_join => $a,
    on      => $a->id == $c->artist,
    where   => $a->name->like('Queen'),
);

foreach (@res) {
    print "Queen Album: ". $_->title ."\n";
}

$track = Track->arow;
$db->do(
    delete_from => $track,
    where       => $track->id == 3,
);

$db->do(
    insert_into => [$track->id, $track->cd, $track->title, $track->length],
    values      => [3, 2, 'Who wants to live forever?', 285]
);

#warn ref($track->length($track->length + 1));

$track = Track->arow;
$db->do(
    update => $track,
    set    => [ $track->id->set(3),
                $track->cd->set(2),
                $track->title->set('Who wants to live forever?'),
                $track->length->set($track->length + 1)
    ],
    where => $track->id == 3,
);


__DATA__
Artist,1,Queen
Artist,2,INXS
CD,1,A Kind of Magic,1986,1
CD,2,A Night at the Opera,1978,2
Track,1,2,Death on Two Legs (Dedicated to.......),223
Track,2,2,Lazing On A Sunday Afternoon,67
Track,3,2,I'm in Love with My Car,187
Track,4,2,You're My Best Friend,170
Track,5,2,39,211
Track,6,2,Sweet Lady,243
Track,7,2,Seaside Rendezvous,135
Track,8,2,The Prophet's Song,501
Track,9,2,Love of My Life,219
Track,10,2,Good Company,203
Track,11,2,Bohemian Rhapsody,355
Track,12,2,God Save the Queen,138
Track,13,2,I'm in Love with My Car,208
Track,14,2,You're My Best Friend,172
Track,15,2,One Vision,310
Track,16,1,A Kind of Magic,264
Track,17,1,One Year of Love,266
Track,18,1,Pain Is So Close to Pleasure,261
Track,19,1,Friends Will Be Friends,247
Track,20,1,Who Wants to Live Forever,305
Track,21,1,Gimme the Prize,274
Track,22,1,Don't Lose Your Head,278
Track,23,1,Princes of the Universe,212
Fan,1,Fan1,100
Fan,2,Fan2,83
Fan,3,Fan3,3
Fan,4,Faker,4
Fan,5,Fan5,52
Fan,6,Fan6,88
Fan,7,Fan7,36
Fan,8,Not a Fan,0
ArtistFan,1,1
ArtistFan,1,2
ArtistFan,1,3
ArtistFan,1,6
ArtistFan,1,7
ArtistFan,2,1
ArtistFan,2,3
ArtistFan,2,5
ArtistFan,2,6
