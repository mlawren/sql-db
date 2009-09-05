package SQL::DB::ARow;
use strict;
use warnings;
use base qw(Class::Accessor);
use Carp qw(carp croak confess cluck);
use SQL::DB::AColumn;
use Scalar::Util qw(weaken);


our $DEBUG;

our $tcount = {};


sub make_class_from {
    my $proto = shift;
    my @colnames = sort map { $_->name } @_;

    no strict 'refs';
    my $aclass = $proto .'::'. $_[0]->table->schema->name .'::'.
        join( '_', $_[0]->table->name, @colnames );

    my $isa = \@{$aclass . '::ISA'};
    if (defined @{$isa}) {
        carp "redefining $aclass";
    }
    push(@{$isa}, 'SQL::DB::ARow');
    $aclass->mk_accessors( @colnames );

    {
        no warnings 'once';
        ${$aclass .'::TABLE'} = $_[0]->table;
    }

    foreach my $colname ( @colnames ) {
        *{$aclass .'::set_'. $colname} = sub {
            my $self = shift;
            return $self->$colname->set(@_);
        };
    }

    return $aclass;
}


sub _getid {
    my $name = shift;
    $tcount->{$name} ||= [];
    my $i = 0;
    while ($tcount->{$name}->[$i]) {
        $i++;
    }
    $tcount->{$name}->[$i] = 1;
    return $i;
}


sub _releaseid {
    my ($name,$i) = @_;
    $tcount->{$name}->[$i] = undef;
    return;
}


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {};
    bless($self, $class);

    $self->{arow_tid} = _getid($self->_table->name);

    foreach my $col ($self->_table->columns) {
        my $acol = SQL::DB::AColumn->new(
            val       => $self->_alias .'.'. $col->name,
            '_column' => $col,
            '_arow'   => $self 
        );

        push(@{$self->{arow_columns}}, $acol);
        $self->{arow_column_names}->{$col->name} = $acol;
        $self->{$col->name} = $acol;
    }
    return $self;
}


sub _table {
    my $self = shift;
    no strict 'refs';
    return ${(ref($self) || $self) . '::TABLE'};
}


sub _table_name {
    my $self = shift;
    return $self->_table->name;
}


sub _alias {
    my $self = shift;
    return $self->_table->name . ($self->{arow_tid}+1);
}


sub _columns {
    my $self = shift;
    return @{$self->{arow_columns}};
}


sub _column_names {
    my $self = shift;
    return $self->_table->column_names;
}


sub _join {
    my $self = shift;
    my $arow = shift || croak '_join($arow)';

    my $t1 = $self->_table;
    my $t2 = $arow->_table;

    foreach my $col ($t1->columns) {
        my $ref = $col->references || next;
        if ($ref->table == $t2) {
            my $colname  = $col->name;
            my $rcolname = $ref->name;
            return ($self->$colname == $arow->$rcolname);
        }
    }

    foreach my $col ($t2->columns) {
        my $ref = $col->references || next;
        if ($ref->table == $t1) {
            my $colname  = $col->name;
            my $rcolname = $ref->name;
            return ($self->$rcolname == $arow->$colname);
        }
    }
    return;
}


sub _join_columns {
    my $self = shift;
    my $arow = shift || croak '_join_columns($arow)';

    my @cols;

    my $t1 = $self->_table;
    my $t2 = $arow->_table;

    foreach my $col ($t1->columns) {
        my $ref = $col->references || next;
        if ($ref->table == $t2) {
            push(@cols, $col->name);
        }
    }

    foreach my $col ($t2->columns) {
        my $ref = $col->references || next;
        if ($ref->table == $t1) {
            push(@cols, $ref->name);
        }
    }

    return @cols;
}


DESTROY {
    my $self = shift;
    _releaseid($self->_table->name, $self->{arow_tid}) if ($self->_table);
    warn "DESTROY $self" if($SQL::DB::DEBUG && $SQL::DB::DEBUG>3);
}


1;
__END__

=head1 NAME

SQL::DB::ARow - Abstract tablerow objects for SQL::DB

=head1 SYNOPSIS

  use SQL::DB::ARow;
  use SQL::DB::Query;

  my $class = SQL::DB::ARow->make_class_from( @SQL::DB::Column );

  my $arow = $class->new;

  my @query = SQL::DB::Query->new(
    select => [ $arow->col1, $arow->col2->as('other') ],
    from   => $arow,
    where  => $arow->col2 == $value,
  )

=head1 DESCRIPTION

L<SQL::DB> provides a low-level interface to SQL databases, using Perl
objects and logic operators. B<SQL::DB::ARow> based objects represent
any row in a table and are use to construct L<SQL::DB> queries. Most
users will not use this module directly.

L<SQL::DB::ARow> has a single class method make_class_from() which
takes a list containing L<SQL::DB::Column> objects and returns a new
class name. The rest of this pod/documentation refers to the generated
classes based on L<SQL::DB::ARow>.

=head1 CONSTRUCTOR

The new() constructor returns a new L<SQL::DB::ARow> based object and
takes no arguments.

=head1 METHODS

=over 4

=item _table -> $SQL::DB::Table

Returns the matching L<SQL::DB::Table> object this object maps to.

=item _table_name -> $name

Returns the name of _table().

=item _alias -> $alias

Returns the unique SQL alias/name given to each instance of the class.

=item _columns -> @SQL::DB::AColumn

Returns the list of L<SQL::DB::AColumn> objects that represent the
table columns.

=item _column_names -> @names

Returns the names of the columns.

=item _join($arow) -> $SQL::DB::Expr

This method takes another B<SQL::DB::ARow> object and returns
an L<SQL::DB::Expr> expression suitable for JOINing the two tables
together based on their foreign key relationships. Eg:

    $db->fetch(
        select    => [$arow1->_columns],
        from      => $arow,
        left_join => $arow2,
        on        => $arow1->_join($arow2),
    );

Be aware that if there is no direct foreign key relationship between
the two 'undef' will be returned and the SQL generated in this example
would be invalid, producing a DBI/DBD error.

=item _join_columns($arow) -> @names

This method takes another B<SQL::DB::ARow> object and returns
the names of the columns of the calling object that would be used for a
join. An empty list is returned if there is no foreign key relationship
between the two tables.

=back

=head1 SEE ALSO

L<SQL::DB>, L<SQL::DB::AColumn>, L<SQL::DB::Table>

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2009 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.

=cut

# vim: set tabstop=4 expandtab:
