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

    warn $pkg if($main::DEBUG);

    if ($col->references) {
        my $t = $col->references->name;

        foreach my $fcol ($col->references->table->columns, '_columns') {
            my $fcolname;
            if (ref($fcol)) {
                $fcolname = $fcol->name;
            }
            else {
                $fcolname = $fcol;
            }

            my $sym = $pkg .'::'. $fcolname;
            *{$sym} = sub {
                my $self = shift;
                if (!$self->{reference}) {
                    my $foreign_row = SQL::DB::ARow->_new(
                        $col->references->table,
                        $self
                    );
                    $self->{reference} = $foreign_row;
                    $self->{arow}->_references(
                        [$foreign_row, ($self == $foreign_row->$t)]
                    );
                }
                return $self->{reference}->$fcolname;
            };
            warn $sym if($main::DEBUG);
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
    weaken($self->{arow});

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
    return $self->{col}->name;
}


sub _arow {
    my $self = shift;
    return $self->{arow};
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


#sub count {
#    my $self = shift;
#    return 'COUNT(' .$self->sql . ')';
#}


sub sql {
    my $self = shift;
    return $self->{arow}->_alias .'.'. $self->{col}->name;
}


DESTROY {
    my $self = shift;
    warn "DESTROY $self" if($main::DEBUG);
}


1;
__END__
