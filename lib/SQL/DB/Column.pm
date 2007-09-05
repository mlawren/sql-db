package SQL::DB::Column;
use strict;
use warnings;
use base qw(Class::Accessor::Fast);
use Carp qw(carp croak);
use Scalar::Util qw(weaken);

SQL::DB::Column->mk_accessors(qw(
    table
    name
    type
    null
    default
    unique
    auto_increment
));


our $DEBUG;


sub table {
    my $self = shift;
    if ( @_ ) {
        my $table = shift;
        unless(CORE::ref($table) and CORE::ref($table) eq 'SQL::DB::Table') {
            croak "table must be a SQL::DB::Table";
        }
        $self->{table} = $table;
        weaken($self->{table});
    }
    return $self->{table};
}


sub primary {
    my $self = shift;
    if (@_) {
        if ($self->{primary} = shift) {
            $self->{table}->add_primary($self);
        }
    }
    else {
        return $self->{primary};
    }
}


#
# This is a delayed value function. Takes a string, but first time
# is accessed it finds the real column and sets itself to that column.
#
sub ref {references(@_);};
sub references {
    my $self = shift;
    # Set a value
    if (@_) {
        $self->{references} = shift;
        return;
    }

    # Not set
    if (!$self->{references}) {
        return;
    }

    # Already accessed - return the reference to SQL::DB::Column
    if (CORE::ref($self->{references})) {
        return $self->{references};
    }

    # Not yet accessed - find the reference to SQL::DB::Column
    my @cols = $self->table->text2cols($self->{references});
    $self->{references} = $cols[0];
    weaken($self->{references});
#   $col->table->has_many($self);
    return $self->{references};
}


sub sql_default {
    my $self = shift;
    my $default = $self->default;
    if (!defined($default)) {
        return '';
    }

    if ($self->type =~ m/(int)|(real)|(float)|(double)|(numeric)/i) {
        return ' DEFAULT ' . $default
    }
    return " DEFAULT '" . $default ."'";
}

sub sql {
    my $self = shift;
    return sprintf('%-15s %-15s', $self->name, $self->type)
           . ($self->null ? 'NULL' : 'NOT NULL')
           . $self->sql_default
           . ($self->auto_increment ? ' AUTO_INCREMENT' : '')
           . ($self->unique ? ' UNIQUE' : '')
#           . ($self->primary ? ' PRIMARY KEY' : '')
           . ($self->references ? ' REFERENCES '
               . $self->references->table->name .'('
               . $self->references->name .')' : '')
    ;
}


DESTROY {
    my $self = shift;
    warn "DESTROY $self" if($DEBUG);
}

1;
__END__
# vim: set tabstop=4 expandtab:
