package SQL::API::Column;
use strict;
use warnings;
use Carp qw(carp croak);
use overload '""' => 'sql';


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        name  => undef,
        table => undef,
        def   => undef,
        @_,
    };


    unless (ref($self->{table}) and ref($self->{table}) eq 'SQL::API::Table') {
        croak 'table must be an SQL::API::Table'; 
    }

    unless (ref($self->{def}) and ref($self->{def}) eq 'HASH') {
        die 'column definition must be a HASHREF'; 
    }

    bless($self, $class);
    $self->setup;
    return $self;
}


sub setup {
    my $self = shift;

    $self->{bind_values}    = [];
    $self->{type}           = delete $self->{def}->{type};
    $self->{null}           = delete $self->{def}->{null};

    if (exists($self->{def}->{default})) {
        $self->{default} = $self->{def}->{default};
        if (defined($self->{default})) {
            push(@{$self->{bind_values}}, $self->{default});
        }

    }
    delete $self->{def}->{default};

    $self->{auto_increment} = delete $self->{def}->{auto_increment};
    $self->{unique}         = delete $self->{def}->{unique};
    delete $self->{def}->{name};

    if (my @leftovers = keys %{$self->{def}}) {
        warn "Unknown Column definition(s): ". join(',', @leftovers);
    }
}



sub name {
    my $self = shift;
    return $self->{name};
}


sub table {
    my $self = shift;
    return $self->{table};
}


# only set by API::Table during table definition
sub foreign_key {
    my $self = shift;
    if (@_) {
        $self->{foreign_key} = shift;
    }
    return $self->{foreign_key};
}


sub sql {
    my $self = shift;
    return sprintf('%-15s %-15s',
            $self->{name},
            $self->{type})
           . ($self->{null} ? 'NULL' : 'NOT NULL')
           . (exists($self->{default}) ? ' DEFAULT '.(defined($self->{default}) ? '?': 'NULL') : '')
           . ($self->{auto_increment} ? " AUTO_INCREMENT" : '')
           . ($self->{unique} ? " UNIQUE" : '')
    ;
}


sub bind_values {
    my $self = shift;
    return @{$self->{bind_values}};
}

1;
__END__
