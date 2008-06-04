use strict;
use warnings;
use Benchmark qw(:all);
use SQL::DB qw(define_tables max min count coalesce sum));

require 't/TestLib.pm';

define_tables(TestLib->All);

TestLib::populate();

my $db = SQL::DB->new;
isa_ok($db, 'SQL::DB');

$db->connect(TestLib->dbi);


my $track   = $db->arow('tracks');
my $cd     = $db->arow('cds');
my $artist = $db->arow('artists');



#$db->do(
#    update => [ $track->set_id(3),
#                $track->set_cd(2),
#                $track->set_title('Who wants to live forever?'),
#                $track->set_length($track->length + 1)
#    ],
#    where => $track->id == 3,
#);

my @objs;



timethese(5000, {
    prepare_cached => sub {
    if (0) {
        @objs = $db->fetch(
            select   => [ $track->id,$track->title,
                        $cd->year,$artist->name ],
            from     => [$track, $cd, $artist],
            distinct => 1,
            where    => ( $track->length < 248 ) &
                    ! ($cd->year > 1997), #&
            order_by => [ $track->title ],
        );
}
        @objs = $db->fetch(
            select   => [ count($track->id)->as('count_id'),
                        $cd->title,
                        max($track->length)->as('max_length'),
                        sum($track->length)->as('sum_length')],
            from       => [$track],
            inner_join => $cd,
            on         => $track->cd == $cd->id,
            group_by   => [$cd->title],
        );
    },
    prepare => sub {
    if (0) {
        @objs = $db->fetch_nopc(
            select   => [ $track->id,$track->title,
                        $cd->year,$artist->name ],
            from     => [$track, $cd, $artist],
            distinct => 1,
            where    => ( $track->length < 248 ) &
                    ! ($cd->year > 1997), #&
            order_by => [ $track->title ],
        );
}
        @objs = $db->fetch_nopc(
            select   => [ count($track->id)->as('count_id'),
                        $cd->title,
                        max($track->length)->as('max_length'),
                        sum($track->length)->as('sum_length')],
            from       => [$track],
            inner_join => $cd,
            on         => $track->cd == $cd->id,
            group_by   => [$cd->title],
        );
    },
});


