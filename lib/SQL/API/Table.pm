package SQL::API::Table;
use strict;
use warnings;
use Carp qw(carp croak);
use SQL::API::Column;

our $tcount = 0;

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        name => undef,
        columns => [],
        @_,
        tid => $tcount++,
    };

    croak 'usage SQL::Table->new(name => $name)' unless($self->{name});

    bless($self, $class);
    $self->_add_columns(@{delete $self->{columns}});
    return $self;
}


sub _name {
    my $self = shift;
    return $self->{name};
}

sub _tid {
    my $self = shift;
    return $self->{tid};
}

sub _add_columns {
    my $self = shift;
    foreach my $col (@_) {
        if ($self->{column_names}->{$col}) {
            croak "Column $col already in table $self->{name}";
        }
        my $newcol = SQL::API::Column->new(name => $col, table => $self);
        push(@{$self->{columns}}, $newcol);
        $self->{column_names}->{$col} = $newcol;
        next;
    }
}


sub _column {
    my $self = shift;
    my $name = shift;
    if (!exists($self->{column_names}->{$name})) {
        croak "Column $name not in table $self->{name}";
    }
    return $self->{column_names}->{$name};
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

sub AUTOLOAD {
    our ($AUTOLOAD);
    no strict 'refs';

    (my $colname = $AUTOLOAD) =~ s/.*:://;
    if (!exists($_[0]->{column_names}->{$colname})) {
        croak "Table $_[0]->{name} doesn't have a column '$colname'";
    }

    *$AUTOLOAD = sub {
        my $self = shift;
        if (!exists($self->{column_names}->{$colname})) {
            croak "Table $self->{name} doesn't have a column $colname";
        }
        return $self->{column_names}->{$colname};
    };

    goto &$AUTOLOAD;
}


1;
__END__
