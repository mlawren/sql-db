package Schema;

sub get { return (
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
    });
}

1;
