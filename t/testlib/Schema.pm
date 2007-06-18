package Schema;

sub get { return (
[
    table   => 'artists',
    columns => [
        [   name => 'id',
            type => 'INTEGER',
            auto_increment => 1,
        ],
        [   name => 'name',
            type => 'VARCHAR(255)',
            unique => 1,
        ],
    ],
    primary =>  [qw(id)],
    unique  =>  [
        [qw(name)]
    ],
    indexes => [
        [
            columns => ['name 10 ASC'],
            unique => 1,
            using => 'BTREE',
        ],
    ],
#        type       =>  'INNODB',          # mysql
#        tablespace =>  'diskvol1',        # postgres
],
[
    table => 'cds',
    columns => [
        [   name => 'id',
            type => 'INTEGER',
            auto_increment => 1,
        ],
        [   name        => 'artist',
            type        => 'INTEGER',
            references  => 'artists(id)',
        ],
        [   name => 'year',
            type => 'INTEGER',
            default => '1997',
        ],
        [   name => 'title',
            type => 'VARCHAR(255)',
            null => 1,
            unique => 1,
        ],
    ],
    primary =>  [qw(id)],
    unique => [
        ['title'],
    ],
    foreign => [
        [
            columns  => [qw(artist)],
            references  => ['artists(id)'],
        ],
    ],
    indexes => [
        [
            columns => [qw(title)],
        ],
    ],
],
[
    table => 'tracks',
    columns => [
        [   name => 'id',
            type => 'INTEGER',
            auto_increment => 1,
        ],
        [   name => 'cd',
            type => 'INTEGER',
            references => 'cds(id)',
        ],
        [   name => 'title',
            type => 'VARCHAR(255)',
            null => 1,
            unique => 1,
        ],
    ],
    primary =>  [qw(id)],
    unique => [
        [qw(cd title)],
    ],
]);

}


1;
