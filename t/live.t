use strict;
use warnings;
use Test::More;
use Scalar::Util qw(refaddr);


BEGIN {
    if (!eval {require DBD::SQLite;1;}) {
        plan skip_all => "DBD::SQLite not installed: $@";
    }
    else {
        plan tests => 4;
    }

}

use_ok('SQL::DB');
require_ok('t/testlib/Schema.pm');

$SQL::DB::DEBUG = 1;
$SQL::DB::ARow::DEBUG = 1;

our $schema;
$schema = SQL::DB::Schema->new(Schema->get) unless($schema);

isa_ok($schema, 'SQL::DB::Schema', 'Schema');

our $db = SQL::DB->connect(
    "dbi:SQLite:/tmp/sqldb$$.db",undef,undef,
#    'dbi:Pg:dbname=test;port=5433', 'rekudos', 'rekudos',
    {PrintError => 0, RaiseError => 1},
    $schema,
) unless($db);

isa_ok($db, 'SQL::DB', 'DB');

$db->deploy;

my $arow = $db->arow('tracks');
$db->delete(
    columns => [$arow->id],
);
$arow = $db->arow('cds');
$db->delete(
    columns => [$arow->id],
);
$arow = $db->arow('artists');
$db->delete(
    columns => [$arow->id],
#        where => $arow->id == 4,
);

while (my $str = <DATA>) {
    chomp($str);
    my @values = split(',',$str);

    my $rem = $db->arow(shift @values);

    my $q = $db->insert(
            columns => [$rem->_columns],
            values  => [@values],
    );
}

my $track = $db->arow('tracks');

my @objs = $db->select(
    columns   => [ $track->id,$track->title,
                  $track->cd->year,$track->cd->artist->name ],
    distinct => 1,
    where    => ( $track->length < 248 ) &
              ! ($track->cd->year > 1997), #&
#                  $track->title->like('%Life%'),
    order_by => [ $track->title ],
#    limit => '2',
);

#  print $query,"\n";

foreach my $obj (@objs) {
    print $obj->id,', ',$obj->title,', ',$obj->name,"\n";
}


$track = $db->arow('tracks');
@objs = $db->select(
    columns   => [ $track->id->func('count'),
                   $track->cd->title,
                   $track->length->func('max'),
                   $track->length->func('sum')],
    group_by  => [$track->cd->title],
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



my $cd = $db->arow('cds');
$db->update(
    columns => [$cd->year],
    set     => [2006],
    where    => $cd->id == 10,
);

my $q =  $schema->select(
    columns   => [ $track->id,$track->title,
                  $track->cd->year,$track->cd->artist->name ],
    distinct => 1,
    where    => ( $track->length < 248 ) &
              ! ($track->cd->year > 1997), #&
#                  $track->title->like('%Life%'),
    order_by => [ $track->title ],
#    limit => '2',>select(
);
my $q2 =  $schema->select(
    columns   => [ $track->id->func('count'),$track->title,
                  $track->cd->year,$track->cd->artist->name ],
    distinct => 1,
    where    => ( $track->length < 248 ) &
              ! ($track->cd->year > 1997), #&
#                  $track->title->like('%Life%'),
    union => $q,
    order_by => [ $track->title ],
#    limit => '2',
);
print $q2;


my $link = $db->arow('artists_fans');
my @res = $db->select(
    columns => [$link->fan->name, $link->fan->craziness],
    where   => $link->artist->name == 'Queen',
);

print "Queen Fans (with craziness)\n";
foreach (@res) {
    print $_->name .' ('.$_->craziness .")\n";
}


my $fan = $db->arow('fans');
$link = $db->arow('artists_fans');
@res = $db->select(
    columns => [$fan->name, $fan->craziness],
    where   => $fan->id->not_in($db->schema->select(columns => [$link->fan->id])),
);

print "Un-Fans\n";
foreach (@res) {
    print $_->name .' ('.$_->craziness .")\n";
}



END {
    unlink "/tmp/sqldb$$.db";
}
__DATA__
artists,1,Queen
artists,2,INXS
cds,1,A Kind of Magic,1986,1
cds,2,A Night at the Opera,1978,1
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
fans,1,Fan1,100
fans,2,Fan2,83
fans,3,Fan3,3
fans,4,Faker,4
fans,5,Fan5,52
fans,6,Fan6,88
fans,7,Fan7,36
fans,8,Not a Fan,0
artists_fans,1,1
artists_fans,1,2
artists_fans,1,3
artists_fans,1,6
artists_fans,1,7
artists_fans,2,1
artists_fans,2,3
artists_fans,2,5
artists_fans,2,6
