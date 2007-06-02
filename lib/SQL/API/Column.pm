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


    if (!ref($self->{table}) or
         !$self->{table}->isa('SQL::API::Table')) {
        croak 'table must be based on SQL::API::Table'; 
    }

    if (!$self->{def} or ref($self->{def} ne 'HASH')) {
        croak 'column definition must be a HASHREF'; 
    }

    bless($self, $class);
    $self->{bind_values}    = [];
    $self->{type}           = delete $self->{def}->{type};
    $self->{null}           = delete $self->{def}->{null};
    $self->{default}        = delete $self->{def}->{default};
    $self->{auto_increment} = delete $self->{def}->{auto_increment};
    $self->{unique}         = delete $self->{def}->{unique};

    if (my @leftovers = keys %{$self->{def}}) {
        carp "Unknown Column definition(s): ". join(',', @leftovers);
    }

    if (defined $self->{default}) {
        push(@{$self->{bind_values}}, $self->{default});
    }
    delete $self->{def};
    return $self;
}



sub name {
    my $self = shift;
    return $self->{name};
}


sub table {
    my $self = shift;
    return $self->{table};
}


sub sql {
    my $self = shift;
    return $self->{name}
           .' '. $self->{type}
           .' '. ($self->{null} ? 'NULL' : 'NOT NULL')
           . ($self->{default} ? ' DEFAULT ?' : '')
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
