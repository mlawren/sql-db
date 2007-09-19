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


    my $i = 0;
    foreach my $def (@methods) {
        # index/position in array
        ${$class.'::_index_'.$def->[0]} = $i;
        push(@{$class.'::_columns'}, $def->[2]);

        if (UNIVERSAL::isa($def->[2], 'SQL::DB::Column')) {
            if ($def->[2]->inflate) {
                push(@{$class.'::_inflate'}, $i);
                *{$class.'::_inflate'.$i} = $def->[2]->inflate;
            }
            if ($def->[2]->inflate) {
                push(@{$class.'::_deflate'}, $i);
                *{$class.'::_deflate'.$i} = $def->[2]->deflate;
            }
        }

        # accessor
        *{$class.'::'.$def->[0]} = sub {
            my $self = shift;
            my $pos  = ${$class.'::_index_'.$def->[0]};
            return $self->[$self->[STATUS]->[$pos]]->[$pos];
        };

        # modifier
        if ($def->[1]) {
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

    *{$class.'::q_update'} = sub {
        my $self = shift;
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

            if ($col->primary and !$where->{$tname}) {
                $where->{$tname} =
                   ($arows->{$tname}->$colname == $self->[ORIGINAL]->[$i]);
            }
            elsif ($col->primary) {
                $where->{$tname} =
                    ($where->{$tname} &
                    ($arows->{$tname}->$colname == $self->[ORIGINAL]->[$i]));
            }

            if ($self->[STATUS]->[$i] == MODIFIED) {
                push(@{$updates->{$tname}},
                    $arows->{$tname}->$colname->set($self->[MODIFIED]->[$i])
                );
            }
            $i++;
        }

        my @query;
        foreach my $table (keys %{$updates}) {
            next unless(@{$updates->{$table}});
            push(@query, [
                update => $updates->{$table},
                ($where->{$table} ? (where  => $where->{$table}) : ()),
            ]);
        }
        return @query;
    };

    return $class;
}


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $valref = shift || croak 'new requires ARRAYREF argument';
    ref($valref) eq 'ARRAY' || croak 'new requires ARRAYREF argument';

    my $self  = [
        $valref,                   # ORIGINAL
        [],                        # MODIFIED
        [map {ORIGINAL} @$valref], # STATUS
    ];
    
    bless($self, $class);
    return $self;
}


sub _inflate {
    my $self = shift;
    my $class = ref($self);

    no strict 'refs';
    foreach my $i (@{$class .'::_inflate'}) {
        my $inflate = *{$class .'::_inflate'.$i};
        $self->[$self->[STATUS]->[$i]]->[$i] =
            &$inflate($self->[$self->[STATUS]->[$i]]->[$i]);
    }
    return $self;
}


sub _deflate {
    my $self = shift;
    my $class = ref($self);

    no strict 'refs';
    foreach my $i (@{$class .'::_deflate'}) {
        my $deflate = *{$class .'::_deflate'.$i};
        $self->[$self->[STATUS]->[$i]]->[$i] =
            &$deflate($self->[$self->[STATUS]->[$i]]->[$i]);
    }
    return $self;
}



1;
__END__
