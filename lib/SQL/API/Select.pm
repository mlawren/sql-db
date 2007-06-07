package SQL::API::Select;
use strict;
use warnings;
use base qw(SQL::API::Query);


sub select {
    my $self = shift;
    $self->{select} = \@_;
    return $self;
}

sub distinct {
    my $self = shift;
    if (ref($_[0])) {
        $self->{distinct} = \@_;
    }
    else {
        $self->{distinct} = shift;
    }
    return $self;
}


sub select_sql {
    my $self = shift;
    my %aliases;

    foreach (@{$self->{select}}) {
        $aliases{$_->_arow->_name} = $_->_arow->_alias;
        foreach ($_->_arow->_foreign_arows) {
            $aliases{$_->_name} = $_->_alias;
        }
    }

    my $s = 'SELECT';

    if ($self->{distinct}) {
        $s .= ' DISTINCT';
        if (ref($self->{distinct}) eq 'ARRAY') {
            foreach (@{$self->{distinct}}) {
                $aliases{$_->_arow->_name} = $_->_arow->_alias;
            }
            $s .= ' ON (' . join(', ', @{$self->{distinct}}) . ')';
        }
    }

    $s .= "\n    " .
            join(",\n    ", @{$self->{select}});

    $s .= "\nFROM\n    " .
            join(",\n    ",
            map {"$_ AS $aliases{$_}"} keys %aliases);

    return $s;
}


sub order_by {
    my $self = shift;
    $self->{order_by} = \@_;
    return $self;
}


sub order_by_sql {
    my $self = shift;
    if ($self->{order_by}) {
        return "ORDER BY\n    " .
               join(",\n    ", @{$self->{order_by}});
    }
    return '';
}


sub sql {
    my $self = shift;

    return
          $self->select_sql
        . $self->where_sql
#        . $self->group_by_sql
#        . $self->having_sql
        . $self->order_by_sql
#        . $self->limit_sql
    ;

}


sub columns {
    my $self = shift;
    return @{$self->{select}};
}

sub column_names {
    my $self = shift;
    return map {$_->name} @{$self->{select}};
}


1;
__END__

