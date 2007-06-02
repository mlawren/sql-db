package SQL::API::Column;
use strict;
use warnings;
use base qw(SQL::API::Expr);
use Carp qw(croak);


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        name => undef,
        table => undef,
        @_,
    };

    if (ref($self->{table}) and !$self->{table}->isa('SQL::API::Table')) {
        croak 'usage SQL::Table->new(name => $name, table => $table)'
    }

    bless($self, $class);
    $self->push_bind_values();
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


sub asc {
    my $self = shift;
    return $self->sql . ' ASC';
}


sub desc {
    my $self = shift;
    return $self->sql . ' DESC';
}


sub sql {
    my $self = shift;
    return 't' . $self->{table}->_tid .'.'. $self->{name};
}


1;
__END__
