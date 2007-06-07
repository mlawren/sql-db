package SQL::API::ARow;
use strict;
use warnings;
use Carp qw(carp croak confess);
use SQL::API::AColumn;

my $ABSTRACT = 'SQL::API::Abstract::';
our $tcount = 0;


sub _define {
    shift;
    my $table = shift;

    no strict 'refs';

    my $pkg = $ABSTRACT . $table->name;
    my $isa = \@{ $pkg . '::ISA'};
    if (defined @{$isa}) {
        carp "redefining $pkg";
    }

    push(@{$isa}, 'SQL::API::ARow');

    warn $pkg if($main::DEBUG);

    foreach my $col ($table->columns) {
        my $sym = $pkg .'::'. $col->name;
        *{$sym} = sub {
            my $self = shift;
            return $self->{column_names}->{$col->name};
        };

#        SQL::API::AColumn->_define($col);
    }
}


sub _new {
    shift;
    my $table = shift;

    #
    # The first time this is called we need to define the package
    #
    my $pkg   = $ABSTRACT . $table->name;
    my $isa   = $pkg .'::ISA';

    if (!defined @{$isa}) {
        __PACKAGE__->_define($table);
    }

    my $self = {
        table        => $table,
        tid          => $tcount++,
    };
    bless($self, $pkg);

    foreach my $col ($self->{table}->columns) {
        my $acol = SQL::API::AColumn->_new($col, $self);

        push(@{$self->{columns}}, $acol);
        $self->{column_names}->{$col->name} = $acol;
    }
    return $self;
}


sub _foreign_arow {
    my $self  = shift;
    my $table = shift;

    if (!exists($self->{foreign_arows}->{$table->name})) {
        my $arow = __PACKAGE__->_new($table);
        $self->{foreign_arows}->{$table->name} = $arow;
    }
    return $self->{foreign_arows}->{$table->name};
}


sub _foreign_arows {
    my $self = shift;
    return map {$self->{foreign_arows}->{$_}}
               keys %{$self->{foreign_arows}};
}

sub _name {
    my $self = shift;
    return $self->{table}->name;
}


sub _table {
    my $self = shift;
    return $self->{table};
}

sub _alias {
    my $self = shift;
    return 't'. $self->{tid};
}


sub _columns {
    my $self = shift;
    if (!@_) {
        return @{$self->{columns}};
    }

    my @cols;
    foreach my $name (@_) {
        if (!exists($self->{column_names}->{$name})) {
            croak "Column $name not in table $self->{name}";
        }
        push(@cols, $self->{column_names}->{$name});
    }
    return @cols;
}


1;
__END__
