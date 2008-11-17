package SQLDBTest;
use strict;
use warnings;
use base qw(SQL::DB);
use File::Temp qw(tempfile tempdir);

#use DBI qw(SQL_BLOB);
#use Storable qw(nfreeze thaw);
#use Encode qw(encode_utf8 decode_utf8);

#our $pg_type;
#BEGIN {
#    if (eval {require DBD::Pg;1;}) {
#        eval '$pg_type = DBD::Pg::PG_BYTEA;';
#    }
#}

my $tempdir = tempdir(DIR => 't/', CLEANUP => 1);
my ($fh,$tfile) = tempfile(DIR => $tempdir);


# Default is SQLite. Other classes can change if they like.
sub test_connect {
    my $self = shift;
    my $ret;
    if ($ENV{TEST_PG}) {
        $ret = $self->connect('dbi:Pg:dbname=test;port=5433',
                              'rekudos', 'rekudos');
    }
    $ret =  $self->connect('dbi:SQLite:'. $tfile, 'user', 'pass');
    $self->_undeploy() unless($_[0]);
    return $ret;
}


sub test_populate {
    my $self = shift;
    $self->deploy();

    my $track   = $self->arow('tracks');
    my $cd     = $self->arow('cds');
    my $artist = $self->arow('artists');

    while (my $str = <DATA>) {
        chomp($str);
        my @values = split(',',$str);

        my $class = shift @values;
        my $rem = $self->arow($class);

        my $q = $self->do(
                insert_into => [$rem->_columns],
                values  => [@values],
        );
    }

}


1;
__DATA__
artists,1,Queen,QUEEN
artists,2,INXS,INXS
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
