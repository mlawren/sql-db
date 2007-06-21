package SQL::DB::Query::Select;
use strict;
use warnings;
use base qw(SQL::DB::Query);
use Carp qw(croak confess carp);


sub distinct {
    my $self = shift;
    $self->{distinct} = shift;
    return;
}


sub select_sql {
    my $self = shift;

    $self->get_aliases(map {$_->_arow} @{$self->{acolumns}});

    my $s = 'SELECT';

    if ($self->{distinct}) {
        $s .= ' DISTINCT';
        if (ref($self->{distinct}) eq 'ARRAY') {
            $self->get_aliases(map {$_->_arow} @{$self->{distinct}});
            $s .= ' ON (' . join(', ', @{$self->{distinct}}) . ')';
        }
    }

    $s .= "\n    " .
            join(",\n    ", map {$_->sql(1)} @{$self->{acolumns}});

    $s .= "\nFROM\n    " .
            join(",\n    ", $self->aliases);

    return $s;
}


sub group_by {
    my $self = shift;
    $self->{group_by} = shift;
    return $self;
}


sub group_by_sql {
    my $self = shift;
    if ($self->{group_by}) {
        return "GROUP BY\n    " .
               join(",\n    ", @{$self->{group_by}});
    }
    return '';
}



sub union {
    my $self = shift;
    my $union = shift;
    unless(ref($union) and $union->isa('SQL::DB::Expr')) {
        confess "Select union must be based on SQL::DB::Expr";
    }
    $self->{union} = $union;
    $self->push_bind_values($union->bind_values);
    return;
}


sub union_sql {
    my $self = shift;
    if ($self->{union}) {
        return "UNION (\n". $self->{union}->sql . "\n)\n";
    }
    return '';
}


sub order_by {
    my $self = shift;
    $self->{order_by} = shift;
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


sub limit {
    my $self = shift;
    $self->{limit} = shift;
    return;
}


sub limit_sql {
    my $self = shift;
    if ($self->{limit}) {
        return "\nLIMIT ". $self->{limit}. "\n";
    }
    return '';
}


sub offset {
    my $self = shift;
    $self->{offset} = shift;
    return;
}


sub offset_sql {
    my $self = shift;
    if ($self->{offset}) {
        return "\nOFFSET ". $self->{offset}. "\n";
    }
    return '';
}


sub sql {
    my $self = shift;

    return
          $self->select_sql
        . $self->where_sql
        . $self->group_by_sql
#        . $self->having_sql
        . $self->union_sql
        . $self->order_by_sql
        . $self->limit_sql
        . $self->offset_sql
    ;

}



1;
__END__

# vim: set tabstop=4 expandtab:
