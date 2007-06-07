#!/usr/bin/perl
#!/usr/bin/speedy
use strict;
use warnings;
use lib 'lib';
use SQL::API;

our $DEBUG = 1;

my @schema = (
    'Artist' => {
        columns => [
            {   name => 'id',
                type => 'INTEGER',
                auto_increment => 1,
            },
            {   name => 'name',
                type => 'VARCHAR(255)',
                unique => 1,
            },
        ],
        primary =>  [qw(id)],
        unique  =>  [qw(name)],
        indexes => [
            {
                columns => ['name 10 ASC'],
                unique => 1,
                using => 'BTREE',
            },
        ],
#        type       =>  'INNODB',          # mysql
#        tablespace =>  'diskvol1',        # postgres
    },
    'CD' => {
        columns => [
            {   name => 'id',
                type => 'INTEGER',
                auto_increment => 1,
            },
            {   name => 'artist',
                type => 'INTEGER',
            },
            {   name => 'year',
                type => 'INTEGER',
                default => '1997',
            },
            {   name => 'title',
                type => 'VARCHAR(255)',
                null => 1,
                unique => 1,
            },
        ],
        primary =>  [qw(id)],
        unique => [
            {
                columns => [qw(title)],
            },
        ],
        foreign => [
            {
                columns  => [qw(artist)],
                references  => ['Artist(id)'],
            },
        ],
        indexes => [
            {
                columns => [qw(title)],
            },
        ],
    },
    'Tracks' => {
        columns => [
            {   name => 'id',
                type => 'INTEGER',
                auto_increment => 1,
            },
            {   name => 'cd',
                type => 'INTEGER',
                foreign => 'CD(id)',
            },
            {   name => 'title',
                type => 'VARCHAR(255)',
                null => 1,
                unique => 1,
            },
        ],
        primary =>  [qw(id)],
        unique => [
            {
                columns => [qw(cd title)],
            },
        ],
    },
);


my $sql = SQL::API->new(@schema);

my $cd  = $sql->row('CD');

my $i = $sql->query(
    insert => [$cd->artist, $cd->year, $cd->title],
    values => ['Queen', 1987, 'A Kind of Magic' ],
);
print $i,"\n";


my $cd2 = $sql->row('CD');

my $q = $sql->query(
    select   => [$cd->_columns],
    where    => ($cd->artist == $cd->artist->id & $cd->artist->id == 23
#    )
#       & $cd->id->in(
#        $sql->query(
#            select   => [$cd2->_columns],
#            distinct => [$cd2->year, $cd2->title],
#            where    => ($cd2->id == 23),
#        )
    ),
    order_by => [$cd->id],
);


print $q;


