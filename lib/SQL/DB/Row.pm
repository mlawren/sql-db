package SQL::DB::Row;
use strict;
use warnings;
use Carp qw(croak);
use constant ORIGINAL => 0;
use constant MODIFIED => 1;
use constant STATUS   => 2;


sub make_class_from {
    my $proto = shift;
    @_ || croak 'make_class_from() requires arguments';

    my @methods;
    my @tablecolumns;
    foreach my $obj (@_) {
        my $method;
        my $set_method;
        if (UNIVERSAL::can($obj, '_column')) {        # AColumn
            ($method = $obj->_as) =~ s/^t\d+\.//;
            $set_method = 'set_'. $method;
            push(@methods, [$method, $set_method, $obj->_column]);
            push(@tablecolumns, $obj->_column->table->name .'.'. $obj->_column->name);
        }
        elsif (UNIVERSAL::can($obj, 'table')) {      # Column
            $method = $obj->name;
            $set_method = 'set_'. $method;
            push(@methods, [$method, $set_method, $obj]);
            push(@tablecolumns, $obj->table->name .'.'. $obj->name);
        }
        elsif (UNIVERSAL::can($obj, '_as')) {         # Expr
            $method = $obj->_as;
            push(@methods, [$method, undef, undef]);
            push(@tablecolumns, $method);
        }
        else {
            croak 'MultiRow takes AColumns, Columns or Exprs: '. ref($obj);
        }
    }

    my $class = $proto .'::'. join('_', @tablecolumns);

    no strict 'refs';
    my $isa = \@{$class . '::ISA'};
    if (defined @{$isa}) {
        return $class;
    }
    push(@{$isa}, $proto);


    my $defaults = {};

    my $i = 0;
    foreach my $def (@methods) {
        # index/position in array
        ${$class.'::_index_'.$def->[0]} = $i;
        push(@{$class.'::_columns'}, $def->[2]);

        if (UNIVERSAL::isa($def->[2], 'SQL::DB::Schema::Column')) {
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
            croak 'Attempt to define column "'.$def->[0].'" twice. '
                  .'You cannot fetch two columns with the same name';
        }

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
            *{$class.'::'.$def->[1]} = sub {
                my $self = shift;
                if (!@_) {
                    croak $def->[1] . ' requires an argument';
                }
                my $pos  = ${$class.'::_index_'.$def->[0]};
                $self->[STATUS]->[$pos] = 1;
                $self->[MODIFIED]->[$pos] = shift;
                return;
            };
        }
        $i++;
    }


    *{$class.'::new_from_arrayref'} = sub {
        my $proto      = shift;
        my $finalclass = ref($proto) || $proto;

        my $valref = shift ||
            croak 'new_from_arrayref requires ARRAYREF argument';
        ref($valref) eq 'ARRAY' ||
            croak 'new_from_arrayref requires ARRAYREF argument';

        my $self  = [
            $valref,                   # ORIGINAL
            [],                        # MODIFIED
            [map {ORIGINAL} (1..scalar @methods)], # STATUS
        ];
    
        bless($self, $finalclass);
        return $self;
    };


    *{$class.'::new'} = sub {
        my $proto = shift;
        my $incoming;


        if (ref($_[0]) and ref($_[0]) eq 'HASH') {
            $incoming = shift;
        }
        else {
            $incoming = {@_};
        }

        my $hash  = {};
        map {$hash->{$_} = $defaults->{$_}} keys %$defaults;
        map {$hash->{$_} = $incoming->{$_}} keys %$incoming;

        my @status = map {ORIGINAL} (1.. scalar @methods);

        my @array = ();
        while (my ($key,$val) = each %$hash) {
            my $i = ${$class.'::_index_'.$key};
            if (defined($i)) {
                $status[$i] = MODIFIED;
                if (ref($val) and ref($val) eq 'CODE') {
                    $array[$i] = &$val;
                }
                else {
                    $array[$i] = $val;
                }
            }
        }

        my $self  = [
            [],                        # ORIGINAL
            \@array,                   # MODIFIED
            \@status,                  # STATUS
        ];
    
        my $finalclass = ref($proto) || $proto;
        bless($self, $finalclass);
        return $self;
    };


    *{$class.'::_modified'} = sub {
        my $self = shift;
        my $colname = shift || croak 'usage: _modified($colname)';
        my $i = ${$class.'::_index_'.$colname};
        defined($i) || croak 'Column "'.$colname.'" is not in '.ref($self);
        return $self->[STATUS]->[$i];
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
        # AColumn->_arow is undefined and SQL::DB::Schema::Query barfs.
        return ($arows, @queries);
    };


    *{$class.'::q_update'} = sub {
        my $self = shift;
        $self->_deflate;

        my @cols = @{$class .'::_columns'};
        
        my $arows   = {};
        my $updates = {};
        my $where   = {};

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

            if ($self->[STATUS]->[$i] == MODIFIED) {
                push(@{$updates->{$tname}},
                    $arows->{$tname}->$colname->set($self->[MODIFIED]->[$i])
                );
            }
            $i++;
        }

        my @queries;
        foreach my $table (keys %{$where}) {
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
        # AColumn->_arow is undefined and SQL::DB::Schema::Query barfs.
        return ($arows, @queries);
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
        # AColumn->_arow is undefined and SQL::DB::Schema::Query barfs.
        return ($arows, @queries);
    };

    return $class;
}


1;
__END__


=head1 NAME

SQL::DB::Row - description

=head1 SYNOPSIS

  use SQL::DB::Row;

=head1 DESCRIPTION

B<SQL::DB::Row> is ...

=head1 METHODS

=head2 make_class_from


=head2 new_from_arrayref

Create a new object from values contained a reference to an ARRAY. The
array values must be in the same order as the definition of the class.

=head2 new



=head2 _inflate



=head2 _deflate



=head1 FILES



=head1 SEE ALSO

L<Other>

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

=cut

# vim: set tabstop=4 expandtab:
