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
        die 'column definition must be a HASHREF'; 
    }

    bless($self, $class);
    $self->{bind_values}    = [];
    $self->{type}           = delete $self->{def}->{type};
    $self->{null}           = delete $self->{def}->{null};
    $self->{default}        = delete $self->{def}->{default};
    $self->{auto_increment} = delete $self->{def}->{auto_increment};
    $self->{unique}         = delete $self->{def}->{unique};
    $self->{foreign}        = delete $self->{def}->{foreign};
    delete $self->{def}->{name};

    if (my @leftovers = keys %{$self->{def}}) {
        warn "Unknown Column definition(s): ". join(',', @leftovers);
    }

    if (defined $self->{default}) {
        push(@{$self->{bind_values}}, $self->{default});
    }

    if ($self->{foreign}) {
        my $table = SQL::API::Table->_table($self->{foreign}->{table});
        if (!$table) {
            die "Table $self->{foreign}->{table} doesn't exist for "
                 ."foreign key ". $self->{table}->_name .".$self->{name}";
        }
        if (!$table->_column($self->{foreign}->{fcolumn})) {
            die 'Column '. $self->{foreign}->{table}
                  .'.'. $self->{foreign}->{fcolumn}
                  . " doesn't exist for foreign key "
                  . $self->{table}->_name .".$self->{name}";
        }
        $self->{foreign_column} = $table->_column($self->{foreign}->{fcolumn});
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


sub foreign {
    my $self = shift;
    return $self->{foreign_column};
}


sub sql {
    my $self = shift;
    return sprintf('%-15s %-15s %-8s',
            $self->{name},
            $self->{type},
            $self->{null} ? 'NULL' : 'NOT NULL')
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
