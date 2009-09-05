package SQL::DB::Row;
use strict;
use warnings;
use Carp qw(carp croak confess);
use Scalar::Util qw(refaddr);

use constant ORIGINAL => 0;
use constant MODIFIED => 1;
use constant STATUS   => 2;


sub make_class_from {
    my $proto = shift;
    @_ || croak 'make_class_from() requires arguments';

    my @methods;
    my @tablecolumns;
    my $arows = {};
    my $schema = '';

    my $i = 0;
    foreach my $obj (@_) {
        my $method;
        my $set_method;
        if (UNIVERSAL::can($obj, '_column')) {        # AColumn
            ($method = $obj->_as) =~ s/(^t\d+\.)|(^\w+\d+\.)//go;
            $set_method = 'set_'. $method;
            push(@methods, [$method, $set_method, $obj->_column]);
            push(@tablecolumns, $obj->_column->name);
            push(@{$arows->{refaddr($obj->_arow)}}, [$i,$obj->_column]);
        }
        elsif (UNIVERSAL::can($obj, 'table')) {      # Column
            $schema ||= ($obj->table->schema->name .'::');
            $method = $obj->name;
            $set_method = 'set_'. $method;
            push(@methods, [$method, $set_method, $obj]);
            push(@tablecolumns, $obj->name);
        }
        elsif (UNIVERSAL::can($obj, '_as')) {         # Expr
            $method = $obj->_as || $obj->as_string;
            $set_method = 'set_'. $method;
            push(@methods, [$method, $set_method, undef]);
            push(@tablecolumns, $method);
        }
        else {
            croak 'MultiRow takes AColumns, Columns or Exprs: '. ref($obj);
        }
        $i++;
    }

    my $class = $proto ."::$schema". join('_', @tablecolumns);

    no strict 'refs';
    my $isa = \@{$class . '::ISA'};
    if (defined @{$isa}) {
        return $class; # already defined
    }
    push(@{$isa}, $proto);

    ${$class.'::_arow_groups'} = [values %$arows] if(keys %$arows);;

    my $defaults = {};

    $i = 0;
    foreach my $def (@methods) {
        # index/position in array
        ${$class.'::_index_'.$def->[0]} = $i;
        push(@{$class.'::_columns'}, $def->[2]);

        if (UNIVERSAL::isa($def->[2], 'SQL::DB::Column')) {
            if ($def->[2]->inflate) {
                push(@{$class.'::_inflate_indexes'}, $i);
                *{$class.'::_inflate'.$i} = $def->[2]->inflate;
            }
            if ($def->[2]->deflate) {
                push(@{$class.'::_deflate_indexes'}, $i);
                *{$class.'::_deflate'.$i} = $def->[2]->deflate;
            }
            if (defined $def->[2]->default) {
                $defaults->{$def->[0]} = $def->[2]->default;
            }
        }

        if (defined(&{$class.'::'.$def->[0]})) {
            confess "Attempt to define column '".$def->[0]."' twice.\n"
                  ."You cannot fetch two columns with the same name.\n"
                  ."Class: $class\n"
                  ."Column names: ". join(',', map {$_->[0]} @methods);
        }

        push(@{$class.'::_column_names'}, $def->[0]);

        # accessor
        *{$class.'::'.$def->[0]} = sub {
            my $self = shift;
            my $pos  = ${$class.'::_index_'.$def->[0]};
            return $self->[$self->[STATUS]->[$pos]]->[$pos];
        };

        # modifier
        if ($def->[1]) {
            if (defined(&{$class.'::'.$def->[1]})) {
                die "double definition";
            }
            if ($def->[2]) { # we have a column - record the modification
                *{$class.'::'.$def->[1]} = sub {
                    my $self = shift;
                    if (!@_) {
                        croak $def->[1] . ' requires an argument';
                    }
                    my $val = shift;
                    my $pos = ${$class.'::_index_'.$def->[0]};
                    $self->[STATUS]->[$pos] = 1;

                    if (my $sub = $def->[2]->set) {
                        $self->[MODIFIED]->[$pos] = &$sub($self,$val);
                    }
                    else {
                        $self->[MODIFIED]->[$pos] = $val;
                    }
                    return;
                };
            }
            else { # no column, just change the value
                *{$class.'::'.$def->[1]} = sub {
                    my $self = shift;
                    if (!@_) {
                        croak $def->[1] . ' requires an argument';
                    }
                    my $pos  = ${$class.'::_index_'.$def->[0]};
                    $self->[ORIGINAL]->[$pos] = shift;
                    return;
                };
            }
        }
        $i++;
    }


    *{$class.'::new_from_arrayref'} = sub {
        my $proto      = shift;
        my $finalclass = ref($proto) || $proto;

        my $valref = shift ||
            confess 'new_from_arrayref requires ARRAYREF argument';
        ref($valref) eq 'ARRAY' ||
            confess 'new_from_arrayref requires ARRAYREF argument';

        my $self  = [
            $valref,                   # ORIGINAL
            [],                        # MODIFIED
            [map {ORIGINAL} (1..scalar @methods)], # STATUS
        ];
    
        bless($self, $finalclass);
        return $self;
    };


    *{$class.'::new'} = sub {
        my $proto      = shift;
        my $finalclass = ref($proto) || $proto;

        my $incoming;
        if (ref($_[0]) and ref($_[0]) eq 'HASH') {
            $incoming = shift;
        }
        else {
            if (@_ % 2) {
                confess "Uneven number of arguments";
            }
            $incoming = {@_};
        }

        # create the object now so it can be used with set_ triggers
        my @status = map {ORIGINAL} (1.. scalar @methods);
        my @original = ();
        my @modified = ();

        my $self  = [
            \@original,                # ORIGINAL
            \@modified,                # MODIFIED
            \@status,                  # STATUS
        ];
    
        bless($self, $finalclass);


        # Set the default values
        while (my ($key,$val) = each %$defaults) {
            my $i = ${$class.'::_index_'.$key};
            if (defined($i)) {
                if (ref($val) and ref($val) eq 'CODE') {
                    $original[$i] = &$val;
                }
                else {
                    $original[$i] = $val;
                }

                # and also preset the incoming values. We do this so
                # that any set_*() triggers that get called below see
                # all of the values that were passed in.
                if (exists($incoming->{$key})) {
                    $original[$i] = $incoming->{$key};
                }
            }
        }

        # Set the incoming values
        while (my ($key,$val) = each %$incoming) {
            # only set keys which actually exist
            if (defined ${$class.'::_index_'.$key}) {
                my $set = 'set_'.$key;
                $self->$set($val);
            }
        }

        return $self;
    };


    *{$class.'::_column_names'} = sub {
        my $self = shift;
        return @{$class.'::_column_names'};
    };


    *{$class.'::_hashref'} = sub {
        my $self = shift;
        my $hashref = {};
        
        my $i = 0;
        foreach my $def (@methods) {
            $hashref->{$def->[0]} = $self->[$self->[STATUS]->[$i]]->[$i];
            $i++;
        }
        return $hashref;
    };


    *{$class.'::_modified'} = sub {
        my $self = shift;
        my $colname = shift || croak 'usage: _modified($colname)';
        my $i = ${$class.'::_index_'.$colname};
        defined($i) || croak 'Column "'.$colname.'" is not in '.ref($self);
        return $self->[STATUS]->[$i];
    };


    *{$class.'::_hashref_modified'} = sub {
        my $self = shift;
        my $hashref = {};
        
        my $i = 0;
        foreach my $def (@methods) {
            if ($self->[STATUS]->[$i] == MODIFIED) {
                $hashref->{$def->[0]} = $self->[MODIFIED]->[$i];
            }
            $i++;
        }
        return $hashref;
    };


    *{$class.'::_inflate'} = sub {
        my $self = shift;

        foreach my $i (@{$class .'::_inflate_indexes'}) {
            my $inflate = *{$class .'::_inflate'.$i};
            next unless(defined($self->[$self->[STATUS]->[$i]]->[$i]));

            $self->[$self->[STATUS]->[$i]]->[$i] =
                &$inflate($self->[$self->[STATUS]->[$i]]->[$i]);
        }
        return $self;
    };


    *{$class.'::_deflate'} = sub {
        my $self = shift;

        foreach my $i (@{$class .'::_deflate_indexes'}) {
            my $deflate = *{$class .'::_deflate'.$i};
            next unless(defined($self->[$self->[STATUS]->[$i]]->[$i]));

            $self->[$self->[STATUS]->[$i]]->[$i] =
                &$deflate($self->[$self->[STATUS]->[$i]]->[$i]);
        }
        return $self;
    };


    *{$class.'::q_insert'} = sub {
        my $self = shift;
        $self->_deflate;

        my @cols = @{$class .'::_columns'};
        
        my $arows   = {};
        my $columns = {};
        my $values  = {};

        my $i = 0;
        foreach my $col (@cols) {
            next unless($col);
            my $status = $self->[STATUS]->[$i];

            my $colname = $col->name;
            my $tname   = $col->table->name;

            if (!exists($arows->{$tname})) {
                $arows->{$tname}   = $col->table->arow();
                $columns->{$tname} = [];
                $values->{$tname}  = [];
            }

            push(@{$columns->{$tname}}, $arows->{$tname}->$colname);
            push(@{$values->{$tname}}, $self->[$status]->[$i]);

            $i++;
        }

        my @queries;
        foreach my $tname (keys %{$columns}) {
            next unless(@{$columns->{$tname}});
            push(@queries, [
                insert => $columns->{$tname},
                values => $values->{$tname},
            ]);
        }
        $self->_inflate;

        # To prevent circular references all AColumns 'weaken' the link
        # to their ARow. This method returns AColumns to the caller, but
        # is the only holder of a strong reference to the ARows belonging
        # to those AColumns. So if we didn't return the ARow as well then
        # AColumn->_arow is undefined and SQL::DB::Query barfs.
        return ($arows, @queries ? @{$queries[0]} : ());
    };


    *{$class.'::q_update'} = sub {
        my $self = shift;
        $self->_deflate;

        my @cols = @{$class .'::_columns'};
        
        my $arows     = {};
        my $primary   = {};
        my $updates   = {};
        my $where     = {};

        if (my $_arow_groups = ${$class.'::_arow_groups'}) {
            my $groupid = 0;
            foreach my $group (@$_arow_groups) {
                foreach my $group_item (@$group) {
                    my ($i,$col) = @$group_item;
                    my $colname  = $col->name;

                    if (!exists($arows->{$groupid})) {
                        $arows->{$groupid}   = $col->table->arow();
                        $updates->{$groupid} = [],
                    }

                    if ($col->primary) {
                        # NULL primary columns can't be used.
                        if (!defined($self->[$self->[STATUS]->[$i]]->[$i])) {
                            next;
                        }

                        $primary->{$groupid} = 1;
                        if (! defined $where->{$groupid}) {
                            $where->{$groupid} =
                                ($arows->{$groupid}->$colname ==
                                $self->[$self->[STATUS]->[$i]]->[$i]);
                        }
                        else {
                            $where->{$groupid} = $where->{$groupid} & 
                                ($arows->{$groupid}->$colname ==
                                $self->[$self->[STATUS]->[$i]]->[$i]);
                        }
                    }

                    if ($self->[STATUS]->[$i] == MODIFIED) {
                        push(@{$updates->{$groupid}},
                            $arows->{$groupid}->$colname->set(
                                        $self->[MODIFIED]->[$i])
                        );
                    }
                }
                $groupid++;
            }
        }
        else {
            my $i = 0;
            foreach my $col (@cols) {
                next unless($col);

                my $colname = $col->name;
                my $tname   = $col->table->name;

                if (!exists($arows->{$tname})) {
                    $arows->{$tname}   = $col->table->arow();
                    $updates->{$tname} = [],
                }

                if ($col->primary) {
                    # NULL primary columns can't be used.
                    if (!defined($self->[$self->[STATUS]->[$i]]->[$i])) {
                        $i++;
                        next;
                    }

                    $primary->{$tname} = 1;
                    if (! defined $where->{$tname}) {
                        $where->{$tname} = ($arows->{$tname}->$colname ==
                            $self->[$self->[STATUS]->[$i]]->[$i]);
                    }
                    else {
                        $where->{$tname} = $where->{$tname} & 
                            ($arows->{$tname}->$colname ==
                            $self->[$self->[STATUS]->[$i]]->[$i]);
                    }
                }

                if ($self->[STATUS]->[$i] == MODIFIED) {
                    push(@{$updates->{$tname}},
                        $arows->{$tname}->$colname->set($self->[MODIFIED]->[$i])
                    );
                }
                $i++;
            }
        }

        my @queries;
        foreach my $table (keys %{$primary}) {
            next unless($where->{$table} and @{$updates->{$table}});
            push(@queries, [
                update => $updates->{$table},
                ($where->{$table} ? (where  => $where->{$table}) : ()),
            ]);
        }
        $self->_inflate;

        # To prevent circular references all AColumns 'weaken' the link
        # to their ARow. This method returns AColumns to the caller, but
        # is the only holder of a strong reference to the ARows belonging
        # to those AColumns. So if we didn't return the ARow as well then
        # AColumn->_arow is undefined and SQL::DB::Query barfs.
        return ($arows, @queries ? @{$queries[0]} : ());
    };


    *{$class.'::q_delete'} = sub {
        my $self = shift;
        $self->_deflate;

        my @cols = @{$class .'::_columns'};
        
        my $arows   = {};
        my $where   = {};

        my $i = 0;
        foreach my $col (@cols) {
            next unless($col);

            my $colname = $col->name;
            my $tname   = $col->table->name;

            if (!exists($arows->{$tname})) {
                $arows->{$tname}   = $col->table->arow();
            }

            if ($col->primary) {
                # NULL primary columns can't be used.
                if (!defined($self->[$self->[STATUS]->[$i]]->[$i])) {
                    $i++;
                    next;
                }

                if (!$where->{$tname}) {
                    $where->{$tname} = ($arows->{$tname}->$colname ==
                        $self->[$self->[STATUS]->[$i]]->[$i]);
                }
                else {
                    $where->{$tname} = $where->{$tname} & 
                        ($arows->{$tname}->$colname ==
                        $self->[$self->[STATUS]->[$i]]->[$i]);
                }
            }

            $i++;
        }

        my @queries;
        foreach my $table (keys %{$arows}) {
            next unless($where->{$table});
            push(@queries, [
                delete => $arows->{$table},
                where  => $where->{$table},
            ]);
        }

        $self->_inflate;

        # To prevent circular references all AColumns 'weaken' the link
        # to their ARow. This method returns AColumns to the caller, but
        # is the only holder of a strong reference to the ARows belonging
        # to those AColumns. So if we didn't return the ARow as well then
        # AColumn->_arow is undefined and SQL::DB::Query barfs.
        return ($arows, @queries ? @{$queries[0]} : ());
    };

    *{$class.'::quickdump'} = sub {
        my $self = shift;

        my $str;

        my $i = 0;
        foreach my $def (@methods) {
            my $key = $def->[0] .($self->[STATUS]->[$i] ? '[m]' : '');
            my $val = $self->[$self->[STATUS]->[$i]]->[$i];
            if (defined($val) and $val =~ /[^[:graph:][:print:]]/) {
                $val = '*BINARY DATA*';
            }
            else {
                $val = (defined($val) ? $val : 'NULL');
            }
            $str .= sprintf("\%-12s = \%s\n", $key, $val);
            $i++;
        }
        return $str;
    };

    return $class;
}


1;
__END__

=head1 NAME

SQL::DB::Row - Perl representation of an SQL table/query row

=head1 SYNOPSIS

  use SQL::DB::Row;

  # Define/create a new class
  my $class = SQL::DB::Row->make_class_from(
    @SQL::DB::Column,
    @SQL::DB::AColumn,
    @SQL::DB::Expr,
  );

  # Create new objects
  my $row1 = $class->new( key => $val );
  my $row2 = $class->new_from_array_ref( \@values );

  # manipulate/interrogate objects
  $row1->set_col1( $newval ); # setter
  $row2->col2;                # getter
  $row2->_column_names;
  $row2->_hashref;
  $row2->_modified('col1');
  $row2->_hashref_modified;
  $row2->_inflate;
  $row2->_deflate;
  $row2->q_insert;
  $row2->q_update;
  $row2->q_delete;
  $row2->quickdump;

=head1 DESCRIPTION

L<SQL::DB> provides a low-level interface to SQL databases, using Perl
objects and logic operators. B<SQL::DB::Row> based objects are returned
by L<SQL::DB> after a successful fetch() from a database. They are also
returned by the L<SQL::DB::Table> row() method when creating new
objects to insert. Most users will not use this module directly.

L<SQL::DB::Row> has only a single class method make_class_from() which
takes a list containing L<SQL::DB::Column>, L<SQL::DB::Acolumn>, or
L<SQL::DB::Expr> objects and returns the new class name. The rest of
this pod/documentation refers to the generated class.

=head1 CONSTRUCTORS

Objects from the new class are arrayref-based (not hashref-based) and
can be instantiated via the new() or new_from_array_ref() constructors.

=head1 METHODS

Objects from the new class have standard accessors (->column) and
modifiers (->set_column) in addition to the following:

=over 4

=item column_names -> @names

List of column names.

=item _hashref -> \%hash

The object as a hashref.

=item _modified('col') -> Bool

If the column has been modified.

=item _hashref_modified -> FIXME

FIXME

=item _inflate -> \CODE

The 'inflate' coderef from the original column definition.

=item _deflate -> \CODE

The 'deflate' coderef from the original column definition.

=item q_insert -> @query

An array suitable for constructing an L<SQL::DB::Query> to insert the
object.

=item q_update -> @query

An array suitable for constructing an L<SQL::DB::Query> to update the
object.

=item q_delete -> @query

An array suitable for constructing an L<SQL::DB::Query> to delete the
object.

=item quickdump -> $text

A nicely formatted object dump.

=back

=head1 SEE ALSO

L<SQL::DB>, L<SQL::DB::Table>, L<SQL::DB::Column>, L<SQL::DB::AColumn>,
L<SQL::DB::Expr>

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright 2007-2009 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.


=cut

# vim: set tabstop=4 expandtab:

