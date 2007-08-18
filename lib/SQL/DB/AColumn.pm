package SQL::DB::AColumn::Func;
use strict;
use warnings;
use base qw(SQL::DB::Expr);
use Carp qw(carp croak confess);

#
# This class pretends to be an SQL::DB::AColumn just enough to be
# able to print the column with whatever function is has been called
# with.
#


sub _new {
    my ($proto,$acol,$func) = @_;
    my $class = ref($proto) || $proto;
    my $self = {
        acol => $acol,
        func => $func,
    };
    bless($self, $class);
    return $self;
}


sub _name {
    my $self = shift;
    return $self->{func} .'_'. $self->{acol}->_name;
}


sub _arow {
    my $self = shift;
    return $self->{acol}->_arow;
}


sub _column {
    my $self = shift;
    return $self->{acol}->_column;
}

sub sql_select {sql(@_)} 
sub sql {
    my $self = shift;
    return uc($self->{func}) . '('. $self->{acol}->sql .')';
}


package SQL::DB::AColumn;
use strict;
use warnings;
use base qw(SQL::DB::Expr);
use Carp qw(carp croak confess);
use Scalar::Util qw(weaken);

my $ABSTRACT = 'SQL::DB::Abstract::';

sub _define {
    shift;
    my $col = shift;

    no strict 'refs';

    my $pkg = $ABSTRACT . $col->table->name .'::'. $col->name;
    my $isa = \@{$pkg . '::ISA'};
    if (defined @{$isa}) {
        carp "redefining $pkg";
    }

    push(@{$isa}, 'SQL::DB::AColumn');

    warn $pkg if($SQL::DB::DEBUG && $SQL::DB::DEBUG>2);

    if ($col->references) {
        my $t = $col->references->name;

        *{$pkg .'::_reference'} = sub {
            my $self = shift;
            if (!$self->{reference}) {
                my $foreign_row = SQL::DB::ARow->_new(
                    $col->references->table,
                    $self
                );
                $self->{reference} = $foreign_row;
                $foreign_row->{has_many}->{$self->{arow}->_name} = $self->{arow};
                $self->{arow}->_references(
                    [$foreign_row, ($self == $foreign_row->$t)]
                );
            }
            return $self->{reference};
        };

        *{$pkg .'::_columns'} = sub {
            my $self = shift;
            return $self->_reference->_columns;
        };

        foreach my $fcol ($col->references->table->columns) {
            my $name = $fcol->name;
            *{$pkg .'::'. $name} = sub {
                my $self = shift;
                return $self->_reference->$name;
            };
            warn $pkg.'::'.$name if($SQL::DB::DEBUG && $SQL::DB::DEBUG>2);
        }
    }
}


sub _new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = $class->SUPER::new;

    my $col   = shift;
    my $arow  = shift;
    $self->{col}  = $col;  # column definition SQL::DB::AColumn
    $self->{arow} = $arow; # abstract representation of a table row

#    weaken($self->{arow}); FIXME or cleanup and remove all weak stuff.

    #
    # The first time this is called we need to define the package
    #
    my $pkg   = $ABSTRACT . $col->table->name .'::'. $col->name;
    my $isa   = $pkg .'::ISA';

    if (!defined @{$isa}) {
        __PACKAGE__->_define($col);
    }

    bless($self, $pkg);

    return $self;
}


sub _column {
    my $self = shift;
    return $self->{col};
}


sub _name {
    my $self = shift;
    if ($self->{func}) {
        return $self->{func} .'_'. $self->{col}->name;
    }
    return $self->{_as} || $self->{col}->name;
}


sub _arow {
    my $self = shift;
    return $self->{arow};
}


sub as {
    my $self = shift;
    $self->{_as} = shift;
    return $self;
}


sub is_null {
    my $self = shift;
    return $self->sql .' IS NULL';
}


sub like {
    my $self = shift;
    my $val  = shift;
    if (ref($val) and $val->isa('SQL::DB::Expr')) {
        return SQL::DB::Expr->new($self .' LIKE '. $val, $val->bind_values);
    }
    my $newexpr =  SQL::DB::Expr->new($self .' LIKE ?', $val);
    return $newexpr;
}

sub expr_not {
    my $self = shift;
    return $self->sql .' NOT NULL';
}


sub asc {
    my $self = shift;
    return $self->sql . ' ASC';
}


sub desc {
    my $self = shift;
    return $self->sql . ' DESC';
}


sub func {
    my $self = shift;
    my $func = shift;
    return SQL::DB::AColumn::Func->_new($self, $func);
}


sub sql {
    my $self = shift;
    return $self->{arow}->_alias .'.'. $self->{col}->name;
}

sub sql_select {
    my $self = shift;
    if ($self->{_as}) {
        return $self->{arow}->_alias .'.'. $self->{col}->name
               .' AS '. $self->{_as};
    }
    return $self->{arow}->_alias .'.'. $self->{col}->name;
} 


DESTROY {
    my $self = shift;
    warn "DESTROY $self" if($SQL::DB::DEBUG && $SQL::DB::DEBUG>3);
}


1;
__END__
# vim: set tabstop=4 expandtab:
