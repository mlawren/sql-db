#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use SQL::API;

SQL::API->define_table(
    'CD' => {
        columns => {
            id => {
                type => 'INTEGER',
                auto_increment => 1,
            },
            artist => {
                type => 'INTEGER',
            },
            year => {
                type => 'INTEGER',
                default => '1997',
            },
            title => {
                type => 'VARCHAR(255)',
                null => 1,
                unique => 1,
            },
        },
        primary =>  [qw(id)],
        indexes => [
            {
                columns => [qw(title)],
            },
        ]
    },
);


SQL::API::Table->_define_table(
    'Artist' => {
        columns => {
            id => {
                type => 'INTEGER',
            },
            name => {
                type => 'VARCHAR(255)',
                unique => 1,
            },
        },
        primary =>  [qw(id)],
        indexes => [
            {
                columns => ['name 10 ASC'],
                using => 'BTREE',
            },
        ]
    },
);


print SQL::API->create('CD')->sql,"\n\n";
print SQL::API->create('CD')->bind_values,"\n\n";
print SQL::API->create('Artist')->sql,"\n";

#my $cd
#
#my $s1 = SQL::API->select($session->id,$session->user);
#$s1->where($session->browser == '127.0.0.1');

__END__
my $s2 = SQL::API->select($users->_columns);
$s2->where(
    ($session->browser->is_null & $users->id->in($s1)) |
    (($users->login == 'mlawren') & $session->browser->is_null)
);

print $s2;
print "    /* ('" . join("', '",$s2->bind_values) . "') */\n";


my $cd = SQL::API::Table->new(
    name    => 'CD',
    columns => [qw(id title artist year)],
);

my $artist = SQL::API::Table->new(
    name    => 'Artist',
    columns => [qw(id name)],
);

my $query = SQL::API->select(
    $cd->_columns,
    $artist->id,
    $artist->name
);

# This can also be $query->distinct(1);
$query->distinct($artist->name,$cd->title);

$query->where(
    (($artist->name == 'Queen') | ($cd->year > 1997))
    & ($cd->artist == $artist->id)
    & $artist->id->in(1,2,3)
#    & $artist->id->in($select)
);

$query->order_by(
    $cd->year->desc,
    $cd->title
);

print $query,"\n";
print "    /* ('" . join("', '",$query->bind_values) . "') */\n";


