#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use SQL::API;
use SQL::API::Table;

my $users = SQL::API::Table->new(
    name    => 'users',
    columns => [qw(id login password)],
);

my $session = SQL::API::Table->new(
    name    => 'session',
    columns => [qw(id browser user lastseen)],
);

my $s1 = SQL::API->select($session->id,$session->user);
$s1->where($session->browser == '127.0.0.1');

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


