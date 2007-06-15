package SQL::DB::Query;
use strict;
use warnings;
use base qw(SQL::DB::Expr);
use overload '""' => 'as_string';
use Carp qw(carp croak);

use Data::Dumper;
$Data::Dumper::Indent = 1;


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = $proto->SUPER::new;
    bless($self, $class);

    $self->{aliases}    = {};
    $self->{conditions} = [];

    my $def = shift;

    while (my ($key,$val) = each %{$def}) {
        if ($self->can($key)) {
            $self->$key($val);
            next;
        }
        carp "unknown INSERT argument: $key";
    }

    $self->multi(1);

    return $self;
}



sub get_aliases {
    my $self    = shift;
    
    foreach my $arow (@_) {
        next if ($self->{aliases}->{$arow->_alias}); # already seen
        $self->{aliases}->{$arow->_alias} = $arow->_name;

        foreach ($arow->_references, map {$_->_arow} $arow->_referenced_by) {
            if (ref($_) eq 'ARRAY') {
                $self->get_aliases($_->[0]);
                push(@{$self->{conditions}}, $_->[1]);
            }
            else {
                $self->get_aliases($_);
            }
        }
    }

}


sub aliases {
    my $self = shift;
    return map {"$self->{aliases}->{$_} AS $_"} sort keys %{$self->{aliases}};
}


sub where {
    my $self = shift;
    $self->{where} = shift;
    if (ref($self->{where}) and $self->{where}->isa('SQL::DB::Expr')) {
        $self->push_bind_values($self->{where}->bind_values);
        $self->{where}->multi(0);
    }
    return $self;
}


sub where_sql {
    my $self = shift;
    my $condition;
    if (my @conditions = @{$self->{conditions}}) {
          $condition = '('. join(") AND\n    (",@conditions) . ')';
    }

    if ($self->{where}) {
        return "\nWHERE\n    "
              . ($condition ? $condition ." AND\n    (" : '')
              . $self->{where} 
              . ($condition ? ')' : '')
              . "\n";
    }
    elsif ($condition) {
        return "\nWHERE\n    $condition\n";
#              . join(") AND\n    (",@conditions) . ")\n";
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


sub exists {
    my $self = shift;
    return SQL::DB::Expr->new('EXISTS ('. $self->sql .')', $self->bind_values);
}


1;
__END__
