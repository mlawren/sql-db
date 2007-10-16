package TestLib;
use strict;
use warnings;
use File::Temp qw(tempfile tempdir);
use DBI qw(SQL_BLOB);

my $dir = tempdir(CLEANUP => 1);
my $schema = 0;

sub dbi {
    my ($fh,$tfile) = tempfile(DIR => $dir);
    return 'dbi:SQLite:'. $tfile;
}


sub Artist {
    [
        table => 'artists',
        class => 'Artist',
#        columns => [
#            [name => 'id',  type => 'INTEGER', primary => 1],
#            [name => 'name',type => 'VARCHAR(255)',unique => 1],
#        ],
        column => [name => 'id',  type => 'INTEGER', primary => 1],
        column => [name => 'name',type => 'VARCHAR(255)',unique => 1],
        unique => 'name',
        index  => [
            columns => 'name',
            unique  => 1,
        ],
        type_mysql => 'InnoDB',
    ];
}


sub Default {
    [
        table => 'defaults',
        class => 'Default',
        column => [name => 'id',  type => 'INTEGER', primary => 1],
        column => [name => 'scalar',type => 'INTEGER', default => 1],
        column => [name => 'sub',type => 'INTEGER', default => sub {1+1}],
        column => [
            name => 'binary',
            type => 'BLOB',
            type_pg => 'BYTEA',
            bind_type_pg => 'pg bind type',
        ],
    ];
}


sub CD {
    [
        table => 'cds',
        class => 'CD',
        columns => [
            [name => 'id', type => 'INTEGER', primary => 1],
            [name => 'title', type => 'VARCHAR(255)'],
            [name => 'year', type => 'INTEGER'],
            [name => 'artist', type => 'INTEGER', references => 'artists(id)'],
        ],
        unique  => 'title,artist',
        index   => [
            columns => 'title',
        ],
        index  => [
            columns => 'artist',
        ],
    ]
}

sub Track {
    [
        table => 'tracks',
        class => 'Track',
        columns => [
            [name => 'id', type => 'INTEGER', primary => 1],
            [name => 'cd', type => 'INTEGER', references => 'cds(id)'],
            [name => 'title', type => 'VARCHAR(255)'],
            [name => 'length', type => 'INTEGER'],
        ],
        unique => 'cd,title,length',
        index  => [
            columns => 'cd',
        ],
    ];
}

sub Fan {
    [
        table => 'fans',
        class => 'Fan',
        columns => [
            [name => 'id', type => 'INTEGER', primary => 1],
            [name => 'name', type => 'VARCHAR(255)'],
            [name => 'craziness', type => 'INTEGER', default => 1],
            [name => 'subcraziness', type => 'INTEGER', default => sub {2;}],
        ],
        unique => 'name',
    ];
}

sub ArtistFan {
    [
        table => 'artists_fans',
        class => 'ArtistFan',
        columns => [
            [name => 'artist', type => 'INTEGER', references => 'artists(id)'],
            [name => 'fan', type => 'INTEGER', references => 'fans(id)'],
        ],
        unique => 'artist,fan',
        index  => [
            columns => 'artist',
        ],
        index  => [
            columns => 'fan',
        ],
    ];
}

    
sub All {
    return (Artist(),Default(),CD(),Track(),Fan(),ArtistFan());
}

1;
