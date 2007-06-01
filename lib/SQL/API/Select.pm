package SQL::API::Select;
use strict;
use warnings;
use base qw(SQL::API::Query);


sub select {
    my $self = shift;
    $self->{select} = \@_;
}

sub distinct {
    my $self = shift;
    if (ref($_[0])) {
        $self->{distinct} = \@_;
    }
    else {
        $self->{distinct} = shift;
    }
}


sub select_sql {
    my $self = shift;
    my %tables;

    foreach (@{$self->{select}}) {
        $tables{$_->table->_name} = $_->table->_tid;
    }

    my $s = "\n".$self->{depth} . "SELECT";

    if ($self->{distinct}) {
        $s .= ' DISTINCT';
        if (ref($self->{distinct}) eq 'ARRAY') {
            foreach (@{$self->{distinct}}) {
                $tables{$_->table->_name} = $_->table->_tid;
            }
            $s .= ' ON (' . join(', ', @{$self->{distinct}}) . ')';
        }
    }

    $s .= "\n$self->{depth}    " .
            join(",\n$self->{depth}    ", @{$self->{select}});

    $s .= "\n$self->{depth}FROM\n$self->{depth}    " .
            join(",\n$self->{depth}    ",
            map {"$_ AS t$tables{$_}"} keys %tables);

    return $s;
}


sub order_by {
    my $self = shift;
    $self->{order_by} = \@_;
}


sub order_by_sql {
    my $self = shift;
    if ($self->{order_by}) {
        return "$self->{depth}ORDER BY\n$self->{depth}    " .
               join(",\n$self->{depth}    ", @{$self->{order_by}});
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


1;
__END__
