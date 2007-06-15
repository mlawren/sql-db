package SQL::DB::ARow;
use strict;
use warnings;
use Carp qw(carp croak confess);
use SQL::DB::AColumn;
use Scalar::Util qw(weaken);

my $ABSTRACT = 'SQL::DB::Abstract::';
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

    push(@{$isa}, 'SQL::DB::ARow');

    warn $pkg if($main::DEBUG);

    foreach my $col ($table->columns) {
        my $sym = $pkg .'::'. $col->name;
        *{$sym} = sub {
            my $self = shift;
            return $self->{column_names}->{$col->name};
        };

#        SQL::DB::AColumn->_define($col);
    }
}


sub _new {
    shift;
    my $table = shift;
    my $referring_column = shift;

    #
    # The first time this is called we need to define the package
    #
    my $pkg   = $ABSTRACT . $table->name;
    my $isa   = $pkg .'::ISA';

    if (!defined @{$isa}) {
        __PACKAGE__->_define($table);
    }

    my $self = {
        table         => $table,
        tid           => $tcount++,
        referenced_by => [],
        references    => [],
    };
    bless($self, $pkg);

    if ($referring_column) {
        $self->{referenced_by} = [$referring_column];
        weaken($self->{referenced_by});
    }

    foreach my $col ($self->{table}->columns) {
        my $acol = SQL::DB::AColumn->_new($col, $self);

        push(@{$self->{columns}}, $acol);
        $self->{column_names}->{$col->name} = $acol;
    }
    return $self;
}


sub _referenced_by {
    my $self = shift;
    if ($self->{referenced_by}) {
        return @{$self->{referenced_by}};
    }
    return;
}


sub _references {
    my $self = shift;
    if (@_) {
        push(@{$self->{references}}, shift);
        return;
    }
    return @{$self->{references}};
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


DESTROY {
    my $self = shift;
    warn "DESTROY $self" if($main::DEBUG);
}

1;
__END__
