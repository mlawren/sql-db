use strict;
use warnings;
use Test::More;

plan tests => 7;

require 't/testlib/Schema.pm';
require SQL::API;

my @schema = Schema->get;
my $sql = SQL::API->new(@schema);

print $sql->table('CD');


