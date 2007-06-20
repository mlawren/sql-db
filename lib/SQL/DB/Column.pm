package SQL::DB::Column;
use strict;
use warnings;
use base qw(Class::Accessor::Fast);
use overload '""' => 'as_string', fallback => 1;
use Carp qw(carp croak);
use Scalar::Util qw(weaken);

SQL::DB::Column->mk_accessors(qw(
    table
    name
    primary
    type
    null
    default
    unique
    auto_increment
    references
));


our $DEBUG;


sub table {
    my $self = shift;
    if ( @_ ) {
        my $table = shift;
        unless(ref($table) and ref($table) eq 'SQL::DB::Table') {
            croak "table must be a SQL::DB::Table";
        }
        $self->{'table'} = $table;
        weaken($self->{'table'});
    }
    return $self->{'table'};
}


sub references {
    my $self = shift;
    if ( @_ ) {
        my $col = shift;
        unless(ref($col) and ref($col) eq 'SQL::DB::Column') {
            croak "reference must be a SQL::DB::Column";
        }
        $self->{'references'} = $col;
        weaken($self->{'references'});
    }
    return $self->{'references'};
}


sub sql {
    my $self = shift;
    return sprintf('%-15s %-15s', $self->name, $self->type)
           . ($self->null ? 'NULL' : 'NOT NULL')
           . (defined($self->default) ? ' DEFAULT '
               . (defined($self->default) ? '?' : 'NULL') : '')
           . ($self->auto_increment ? ' AUTO_INCREMENT' : '')
           . ($self->unique ? ' UNIQUE' : '')
           . ($self->primary ? ' PRIMARY KEY' : '')
           . ($self->references ? ' REFERENCES '
               . $self->references->table->name .'('
               . $self->references->name .')' : '')
    ;
}


sub bind_values {
    my $self = shift;
    if ($self->default) {
        return ($self->default);
    }
    return;
}


sub as_string {
    my $self = shift;
    return $self->table->name .'.'. $self->name;
}


DESTROY {
    my $self = shift;
    warn "DESTROY $self" if($DEBUG);
}

1;
__END__
# vim: set tabstop=4 expandtab:
