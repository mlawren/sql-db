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
            join(",\n    ", @{$self->{acolumns}});

    $s .= "\nFROM\n    " .
            join(",\n    ", $self->aliases);

    return $s;
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


sub sql {
    my $self = shift;

    return
          $self->select_sql
        . $self->where_sql
#        . $self->group_by_sql
#        . $self->having_sql
        . $self->order_by_sql
        . $self->limit_sql
    ;

}



1;
__END__

