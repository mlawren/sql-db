package SQL::DB::Function;
use strict;
use warnings;
use overload '""' => 'as_string', fallback => 1;
use Carp qw(croak confess);
use Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    coalesce
    count
    max
    min
    sum
    nextval
    currval
    setval
);


# Class subroutines

sub coalesce {
    my $obj = {
        cols => \@_,
        sql  => sub {
            my $self = shift;
            die "missing AS for Coalesce" unless($self->{as});

            return 'COALESCE('
                . join(', ', @{$self->{cols}})
                . ') AS '
                . $self->{as};
        }
    };

    bless($obj, __PACKAGE__);
    return $obj;
}


sub count {
    my $obj = {
        expr => shift, 
        sql  => sub {
            my $self = shift;
            return 'COUNT('. $self->{expr} .')'
                . ($self->{as} ? ' AS '. $self->{as} : '');
        }
    };

    bless($obj, __PACKAGE__);
    return $obj;
}


sub max {
    my $obj = {
        expr => shift, 
        sql  => sub {
            my $self = shift;
            return 'MAX('. $self->{expr} .')'
                . ($self->{as} ? ' AS '. $self->{as} : '');
        }
    };

    bless($obj, __PACKAGE__);
    return $obj;
}


sub min {
    my $obj = {
        expr => shift, 
        sql  => sub {
            my $self = shift;
            return 'MIN('. $self->{expr} .')'
                . ($self->{as} ? ' AS '. $self->{as} : '');
        }
    };

    bless($obj, __PACKAGE__);
    return $obj;
}


sub sum {
    my $obj = {
        expr => shift, 
        sql  => sub {
            my $self = shift;
            return 'SUM('. $self->{expr} .')'
                . ($self->{as} ? ' AS '. $self->{as} : '');
        }
    };

    bless($obj, __PACKAGE__);
    return $obj;
}


sub nextval {
    my $obj = {
        expr => shift, 
        sql  => sub {
            my $self = shift;
            return 'nextval(\''. $self->{expr} .'\')';
        }
    };

    bless($obj, __PACKAGE__);
    return $obj;
}


sub currval {
    my $obj = {
        expr => shift, 
        sql  => sub {
            my $self = shift;
            return 'currval(\''. $self->{expr} .'\')';
        }
    };

    bless($obj, __PACKAGE__);
    return $obj;
}


sub setval {
    my $obj = {
        expr => [@_], 
        sql  => sub {
            my $self = shift;
            if (@{$self->{expr}} == 2) {
                return 'setval(\''. $self->{expr}->[0] .'\', '.
                                    $self->{expr}->[1] .')';
            }
            elsif (@{$self->{expr}} == 3) {
                return 'setval(\''. $self->{expr}->[0] .'\', '.
                                    $self->{expr}->[1] .', '.
                                   ($self->{expr}->[2] ? 'true' : 'false') .')';
            }
            else {
                confess 'setval() takes 2 or 3 arguments';
            }
        }
    };

    bless($obj, __PACKAGE__);
    return $obj;
}



# Object methods

sub as {
    my $self = shift;
    $self->{as} = shift;
    return $self;
}


sub _arow {
    my $self = shift;
    return '(none)';
}

sub _column {
    my $self = shift;
    return '(none)';
}

sub _name {
    my $self = shift;
    return $self->{as};
}


sub as_string {
    my $self = shift;
    my $sub = $self->{sql};
    if (ref($sub) and ref($sub) eq 'CODE') {
        return &$sub($self);
    }
    else {
        croak "self->{sql} not a CODEREF";
    }
}


1;
__END__
# vim: set tabstop=4 expandtab:
