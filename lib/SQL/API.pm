package SQL::API;
use 5.006;
use strict;
use warnings;
use Carp qw(carp croak);

use SQL::API::Table;
use SQL::API::Insert;
use SQL::API::Select;
#use SQL::API::Update;
#use SQL::API::Delete;

our $VERSION = '0.01';


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        tables => {},
    };
    bless($self,$class);

    my @items;
    if (ref($_[0]) eq 'ARRAY') {
         @items = @{$_[0]};
    }
    else {
         @items = @_;
    }

    while (@_) {
        my $name = shift;
        my $def  = shift;
        $self->define($name, $def);
    }
    return $self;
}


sub define {
    my $self       = shift;
    my $name       = shift;
    my $definition = shift;

    if (!$name or ref($definition) ne 'HASH') {
        croak 'usage: define($name, $hashref)';
    }

    if (exists($self->{tables}->{$name})) {
        warn "Redefining table '$name'";
    }

    $self->{tables}->{$name} = SQL::API::Table->new($name, $definition, $self);
    return $self->{tables}->{$name};
}


# class method
sub table {
    my $self = shift;
    my $name  = shift;

    if (!$name) {
        croak 'usage table($name)';
    }

    if (!exists($self->{tables}->{$name})) {
        croak "Table '$name' has not been defined";
    }
    return $self->{tables}->{$name};
}


sub deploy {
    my $self = shift;
    my $dbi  = shift;
    die 'deploy not implemented yet.'
}


sub row {
    my $self = shift;
    my $name = shift;
    if (!$name) {
        croak 'usage table($name)';
    }
    if (!exists($self->{tables}->{$name})) {
        croak "Table '$name' has not been defined";
    }
    
    return $self->{tables}->{$name}->abstract_row;
}


sub query {
    my $self = shift;

    my $def;
    unless (ref($_[0]) and ref($_[0]) eq 'HASH') {
        my %def = @_;
        $def = \%def;        
    }
    else {
        $def = shift;
    }
#use Data::Dumper;
#$Data::Dumper::Indent=1;

#die ref($def);
    if (exists($def->{insert})) {
        return SQL::API::Insert->new($def);
    }
    elsif (exists($def->{select})) {
        return SQL::API::Select->new($def);
    }
    elsif (exists($def->{update})) {
        return SQL::API::Update->new($def);
    }
    elsif (exists($def->{delete})) {
        return SQL::API::Delete->new($def);
    }

    croak 'query badly defined (missing select,insert,update etc)';
}


1;
__END__

=head1 NAME

SQL::API - Perl extension for writing SQL statements

=head1 SYNOPSIS

  use SQL::API;

  my $cd = SQL::API->table(
      name    => 'CD',
      columns => [qw(id title artist year)],
  );

  my $artist = SQL::API->table(
      name    => 'Artist',
      columns => [qw(id name)],
  );

  my $select = SQL::API->select(
      $cd->_columns,
      $artist->id,
      $artist->name
  )->where(
      ($cd->artist == $artist->id) &
      ($artist->name == 'Queen') &
      ($cd->year > 1997)
  )->order_by(
      $cd->year->desc,
      $cd->title->asc
  );

  my $sth = $dbi->prepare($select->sql);
  $dbi->execute($sth, $select->bind_values);

=head1 DESCRIPTION

B<SQL::API> lets you write SQL using a combination of Perl objects,
method calls and standard logic operators such as '!', '&', '|',...

=head1 FUNCTIONS

Most B<SQL::API> functions are shortcuts to SQL::API::* objects.

=head2 table(name => $name, $columns => [$c1,$c2,$c3])

Returns an object representing an SQL table and its columns. The colums
(as methods of the table object) can be used in SELECT and WHERE statements.

See L<SQL::API::Table> for details.

=head2 create($table)

Returns an object representing the CREATE statements needed to create
table $table.

See L<SQL::API::Create> for details.

=head2 insert($c1,$c2,$c3,...)

Returns an object representing the INSERT statement for
columns $c1,$c2,$c3. Columns must all be from the same table.

See L<SQL::API::Insert> for details.

=head2 select($c1,$c2,$c3)

Returns an object representing a SELECT query which returns columns
@columns.

See L<SQL::API::Select> for details.

=head2 update($c1,$c2,$c3,...)

Returns an object representing an SQL UPDATE. Columns $c1,$c2,$c3
must all be from the same table.

See L<SQL::API::Update> for details.

=head2 delete($table1, $table2, ...)

Returns an object representing an SQL DELETE statement.

See L<SQL::API::Delete> for details.

=head1 SEE ALSO

L<SQL::Builder>, L<SQL::Abstract>

L<Tangram> has some good examples of the query syntax.

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

Feel free to let me know if you find this module useful.

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

=cut

