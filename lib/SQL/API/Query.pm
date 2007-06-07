package SQL::API::Query;
use strict;
use warnings;
use base qw(SQL::API::Expr);
use overload '""' => 'as_string';
use Carp qw(carp croak);


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = $proto->SUPER::new;
    bless($self, $class);

    my $def = shift;
    my %defs;
    unless (ref($def) and ref($def) eq 'HASH') {
        %defs = @_;
    }
    else {
        %defs = %{$def};
    }

    while (my ($key,$val) = each %defs) {
        if ($self->can($key)) {
            if (ref($val) and ref($val) eq 'ARRAY') {
                $self->$key(@{$val});
            }
            else {
                $self->$key($val);
            }
            next;
        }
        carp "unknown argument: $key";
    }

    $self->multi(1);

    return $self;
}


sub where {
    my $self = shift;
    $self->{where} = shift;
    if (ref($self->{where}) and $self->{where}->isa('SQL::API::Expr')) {
        $self->push_bind_values($self->{where}->bind_values);
        $self->{where}->multi(0);
    }
    return $self;
}


sub where_sql {
    my $self = shift;
    if ($self->{where}) {
        return "\nWHERE\n    " .
                 $self->{where} . "\n";
    }
    return '';
}


sub bind_values_sql {
    my $self = shift;
    if ($self->bind_values) {
        return q{/* ('} . join("', '",$self->bind_values) . q{') */};
    }
    return '';
}


sub as_string {
    my $self = shift;
    my @values = $self->bind_values;
    return $self->sql . "\n" . $self->bind_values_sql . "\n";
}


1;
__END__
