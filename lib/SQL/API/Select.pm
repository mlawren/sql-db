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

    $self->get_aliases(map {$_->_arow} @{$self->{select}});

    my $s = 'SELECT';

    if ($self->{distinct}) {
        $s .= ' DISTINCT';
        if (ref($self->{distinct}) eq 'ARRAY') {
            $self->get_aliases(map {$_->_arow} @{$self->{distinct}});
            $s .= ' ON (' . join(', ', @{$self->{distinct}}) . ')';
        }
    }

    $s .= "\n    " .
            join(",\n    ", @{$self->{select}});

    $s .= "\nFROM\n    " .
            join(",\n    ", $self->aliases);

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

