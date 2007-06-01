package SQL::API::Query;
use strict;
use warnings;
use base qw(SQL::API::Expr);
use overload '""' => 'sql';
use Carp qw(croak);


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = bless({}, $class);
    $self->multi(1);
    $self->{depth} = '    ';
    return $self;
}


sub where {
    my $self = shift;
    $self->{where} = shift;
    if (ref($self->{where}) and $self->{where}->isa('SQL::API::Expr')) {
        $self->push_bind_values($self->{where}->bind_values);
        $self->{where}->multi(0);
    }
    return;
}


sub where_sql {
    my $self = shift;
    if ($self->{where}) {
        return "\n$self->{depth}WHERE\n$self->{depth}    " .
                 $self->{where} . "\n";
    }
    return '';
}


1;
__END__
