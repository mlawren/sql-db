package Schema;

sub get { return (
[
    table => 'artists',
    columns => [
        [name => 'id',  type => 'INTEGER', primary => 1],
        [name => 'name',type => 'VARCHAR(255)',unique => 1],
    ],
    unique => 'name',
    index  => [
        columns => 'name',
        unique  => 1,
#        using   => 'BTREE', # postgres only?
    ],
],
[
    table => 'cds',
    columns => [
        [name => 'id', type => 'INTEGER', primary => 1],
        [name => 'title', type => 'VARCHAR(255)'],
        [name => 'year', type => 'INTEGER'],
        [name => 'artist', type => 'INTEGER', references => 'artists(id)'],
    ],
    unique  => 'title',
    foreign => [ artist => 'artists(id)' ],
    index   => [
        columns => 'title',
    ],
    index  => [
        columns => 'artist',
    ],
],
[
    table => 'tracks',
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
],
[
    table => 'fans',
    columns => [
        [name => 'id', type => 'INTEGER', primary => 1],
        [name => 'name', type => 'VARCHAR(255)'],
        [name => 'craziness', type => 'INTEGER'],
    ],
    unique => 'name',
],
[
    table => 'artists_fans',
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
],


);}
    
1;
