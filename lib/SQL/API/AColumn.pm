package SQL::API::AColumn;
use strict;
use warnings;
use base qw(SQL::API::Expr);
use Carp qw(carp croak confess);


my $ABSTRACT = 'SQL::API::Abstract::';

sub _define {
    shift;
    my $col = shift;

    no strict 'refs';

    my $pkg = $ABSTRACT . $col->table->name .'::'. $col->name;
    my $isa = \@{$pkg . '::ISA'};
    if (defined @{$isa}) {
        carp "redefining $pkg";
    }

    push(@{$isa}, 'SQL::API::AColumn');

    warn $pkg if($main::DEBUG);

    if ($col->references) {
        foreach my $fcol ($col->references->table->columns) {
            my $fcolname = $fcol->name;
            my $sym = $pkg .'::'. $fcolname;
            *{$sym} = sub {
                my $self = shift;
                if (!$self->{reference}) {
                    $self->{reference} = SQL::API::ARow->_new(
                        $col->references->table,
                        $self
                    );
                    $self->{arow}->_references($self->{reference});
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
    $self->{col}  = $col;  # column definition SQL::API::AColumn
    $self->{arow} = $arow; # abstract representation of a table row

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


sub _name {
    my $self = shift;
    return $self->{col}->name;
}

sub _arow {
    my $self = shift;
    return $self->{arow};
}

sub asc {
    my $self = shift;
    return $self->sql . ' ASC';
}


sub desc {
    my $self = shift;
    return $self->sql . ' DESC';
}

sub sql {
    my $self = shift;
    return $self->{arow}->_alias .'.'. $self->{col}->name;
}


1;
__END__
