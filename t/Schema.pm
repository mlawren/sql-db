package Schema;

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
            [name => 'craziness', type => 'INTEGER'],
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
    return (Artist(),CD(),Track(),Fan(),ArtistFan());
}

1;
