package SQL::DB::Column;
use strict;
use warnings;
use Carp qw(carp croak);
use Scalar::Util qw(weaken);
use overload '""' => 'as_string';

use Data::Dumper;
$Data::Dumper::Indent = 1;

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        table => undef,
        def   => undef,
        @_,
    };


    unless (ref($self->{table}) and ref($self->{table}) eq 'SQL::DB::Table') {
        croak 'table must be an SQL::DB::Table'; 
    }

    weaken($self->{table});

    unless (ref($self->{def}) and ref($self->{def}) eq 'HASH') {
        die 'column definition must be a HASHREF'; 
    }

    bless($self, $class);
    $self->setup;
    return $self;
}


sub setup {
    my $self = shift;

    $self->{name}           = delete $self->{def}->{name};
    $self->{bind_values}    = [];
    $self->{type}           = delete $self->{def}->{type};
    $self->{null}           = delete $self->{def}->{null};

    if (exists($self->{def}->{default})) {
        $self->{default} = $self->{def}->{default};
        if (defined($self->{default})) {
            push(@{$self->{bind_values}}, $self->{default});
        }

        delete $self->{def}->{default};
    }

    $self->{auto_increment} = delete $self->{def}->{auto_increment};
    $self->{unique}         = delete $self->{def}->{unique};
    $self->{primary}        = delete $self->{def}->{primary};

    if (exists($self->{def}->{references})) {
        my @cols = $self->table->text2cols(
            delete $self->{def}->{references}
        );
        if (@cols > 1) {
            croak "Too many foreign keys for ".$self->table.'.'.$self->{name};
        }
        $self->{references} = $cols[0];
        weaken($self->{references});
    }

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


sub references {
    my $self = shift;
    return $self->{references};
}


sub sql {
    my $self = shift;
#    if (!$self->{type}) {
#        return $self->{name};
#    }
    return sprintf('%-15s %-15s',
            $self->{name},
            ($self->{type} ? $self->{type} : '<type>'))
           . ($self->{null} ? 'NULL' : 'NOT NULL')
           . (exists($self->{default}) ? ' DEFAULT '.(defined($self->{default}) ? '?': 'NULL') : '')
           . ($self->{auto_increment} ? " AUTO_INCREMENT" : '')
           . ($self->{unique} ? " UNIQUE" : '')
           . ($self->{primary} ? " PRIMARY" : '')
           . ($self->{references} ? ' REFERENCES '
                .$self->{references}->table->name .'('
                .$self->{references}->name .')' : '')
    ;
}


sub bind_values {
    my $self = shift;
    return @{$self->{bind_values}};
}


sub as_string {
    my $self = shift;
    return $self->{table}->name .'.'. $self->{name};
}


DESTROY {
    my $self = shift;
    warn "DESTROY $self" if($main::DEBUG);
}

1;
__END__
