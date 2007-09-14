package SQL::DB::Query;
use strict;
use warnings;
use base qw(SQL::DB::Expr);
use Carp qw(carp croak confess);
use UNIVERSAL qw(isa);


#
# A new query - could be insert,select,update or delete
#
sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $proto->SUPER::new; # Get an Expr-based object
    bless($self, $class);

    $self->{is_select} = $_[0] =~ m/^select/i;

    unless (@_) {
        confess "usage: ". __PACKAGE__ ."->new(\@statements)";
    }

    $self->{query} = [];

    while (my $key = shift) {
        my $action = 'st_'.$key;

        unless($self->can($action)) {
            confess "Unknown command: $key";
        }

        my $val    = shift;
        $self->$action($val);
    }

    $self->multi(1);

    if (wantarray()) {
        return ("$self", $self->bind_values);
    }
    return $self;
}


sub push_bind_values {
    my $self = shift;
    my @values;
    foreach my $item (@_) {
        if (ref($item) && UNIVERSAL::isa($item, 'SQL::DB::Object')) {
            my @cols = $item->_table->primary_columns;
            my $colname = $cols[0]->name;
            push(@values, $item->$colname);
        }
        else {
            push(@values, $item);
        }
    }
    $self->SUPER::push_bind_values(@values);
}


sub wantobjects {
    my $self = shift;
    return $self->{selecto};
}


sub arows {
    my $self = shift;
    if ($self->{arows}) {
        return @{$self->{arows}};
    }
    return;
}


sub acolumns {
    my $self = shift;
    if ($self->{acolumns}) {
        return @{$self->{acolumns}};
    }
    return;
}


sub column_names {
    my $self = shift;
    if ($self->{columns}) {
        return map {$_->name} @{$self->{columns}};
    }
    return;
}


sub exists {
    my $self = shift;
    return SQL::DB::Expr->new('EXISTS ('. $self .')', $self->bind_values);
}


sub as_string {
    my $self = shift;
    my @statements = @{$self->{query}};

    my $s;
    while (my ($stm,$val) = splice(@statements,0,2)) {
        $s .= $self->$stm($val);    
    }
    unless ($self->{is_select}) {
        $s =~ s/t\d+\.//g;
    }
    return $s;
}


# ------------------------------------------------------------------------
# WHERE - used in SELECT, UPDATE, DELETE
# ------------------------------------------------------------------------

sub st_where {
    my $self  = shift;
    my $where = shift;

    push(@{$self->{query}}, 'sql_where', $where);
    $self->push_bind_values($where->bind_values);
    return;
}


sub sql_where {
    my $self  = shift;
    my $where = shift;
    if (!$self->{acolumns}) {
        $where =~ s/t\d+\.//g;
    }
    return "WHERE\n    " . $where . "\n";
}



# ------------------------------------------------------------------------
# INSERT
# ------------------------------------------------------------------------

sub st_insert_into {st_insert(@_)};
sub st_insert {
    my $self = shift;
    my $ref  = shift;

#    if (@{$self->{arows}} > 1) {
#        confess "Can only insert into columns of the same table";
#    }

    push(@{$self->{query}}, 'sql_insert', $ref);

    return;
}

sub sql_insert {
    my $self = shift;
    my $ref  = shift;

    return "INSERT INTO\n    ". $ref->[0]->_arow->_table_name
           . ' ('
           . join(', ', map {$_->_name} @{$ref})
           . ")\n";
}


sub st_values {
    my $self = shift;
    my $ref  = shift;

#    if (@{$self->{arows}} > 1) {
#        confess "Can only insert into columns of the same table";
#    }

    push(@{$self->{query}}, 'sql_values', $ref);
    $self->push_bind_values(@{$ref});

    return;
}


sub sql_values {
    my $self = shift;
    my $ref  = shift;

    return "VALUES\n    ("
           . join(', ', map {'?'} @{$ref})
           . ")\n"
    ;
}


# ------------------------------------------------------------------------
# UPDATE
# ------------------------------------------------------------------------

sub st_update {
    my $self = shift;
    my $ref  = shift;

    my @items = (UNIVERSAL::isa($ref,'ARRAY') ? @$ref : $ref);
    foreach (@items) {
        if (UNIVERSAL::isa($_, 'SQL::DB::Expr')) {
            $self->push_bind_values($_->bind_values);
        }
    }

    push(@{$self->{query}}, 'sql_update', $items[0]->_arow->_table_name);
    push(@{$self->{query}}, 'sql_set', \@items);

    return;
}

sub sql_update {
    my $self = shift;
    my $name = shift;

    return "UPDATE\n    " . $name . "\n";
}

sub sql_set {
    my $self = shift;
    my $ref  = shift;

    return "SET\n    " . join(', ',@$ref) . "\n";
}



# ------------------------------------------------------------------------
# SELECT
# ------------------------------------------------------------------------
sub st_select {
    my $self = shift;
    my $ref  = shift;

    my @items    = ref($ref) eq 'ARRAY' ? @{$ref} : $ref;
    my @acolumns = map {UNIVERSAL::isa($_, 'SQL::DB::ARow') ? $_->_columns : $_} @items;

    push(@{$self->{acolumns}}, @acolumns);
    push(@{$self->{query}}, 'sql_select', undef);

    return;
}


sub st_selecto {
    my $self = shift;
    my $ref  = shift;

    $self->{selecto} = 1;
    return $self->st_select($ref);
}


sub sql_select {
    my $self = shift;
    my $ref  = shift;
    my $distinct = $self->{distinct};

    my $s = 'SELECT';
    if ($distinct) {
        $s .= ' DISTINCT';
        if (ref($distinct) and ref($distinct) eq 'ARRAY') {
            $s .= ' ON (' . join(', ', @{$distinct}) . ')';
        }
    }

    # The columns to select
    $s .= "\n    " .join(",\n    ", @{$self->{acolumns}});

    return $s ."\n";
}


sub st_distinct {
    my $self = shift;
    $self->{distinct} = shift;
    return;
}


sub st_for_update {
    my $self = shift;
    my $update = shift;
    if ($update) {
        push(@{$self->{query}}, 'sql_for_update', $update);
    }
    return;
}


sub sql_for_update {
    my $self = shift;
    my $update = shift;
    return "FOR UPDATE\n" ;
}


sub st_from {
    my $self = shift;
    my $ref  = shift;
    my @acols;

    if (UNIVERSAL::isa($ref, 'ARRAY')) {
        foreach (@{$ref}) {
            if (UNIVERSAL::isa($_, 'SQL::DB::AColumn')) {
                push(@acols, $_->_reference);
            }
            elsif (UNIVERSAL::isa($_, 'SQL::DB::ARow')) {
                push(@acols, $_);
            }
            else {
                croak "Invalid from object: $_";
            }
        }
    }
    elsif (UNIVERSAL::isa($ref, 'SQL::DB::AColumn')) {
        push(@acols, $ref->_arow);
    }
    elsif (UNIVERSAL::isa($ref, 'SQL::DB::ARow')) {
        push(@acols, $ref);
    }
    else {
        croak "Invalid from object: $ref";
    }

    push(@{$self->{query}}, 'sql_from', \@acols);
    return;
}


sub sql_from {
    my $self = shift;
    my $ref  = shift;

    return "FROM\n    ". join(",\n    ",
                     map {$_->_table_name. ' AS '. $_->_alias} @{$ref}) ."\n";
}


sub st_on {
    my $self = shift;
    my $val  = shift;
    push(@{$self->{query}}, 'sql_on', $val);
    $self->push_bind_values($val->bind_values);
    return;
}


sub sql_on {
    my $self = shift;
    my $val  = shift;
    return "ON\n    " . $val . "\n";
}


sub st_inner_join {
    my $self = shift;
    my $arow  = shift;
    push(@{$self->{query}}, 'sql_inner_join', $arow);
    return;
}


sub sql_inner_join {
    my $self = shift;
    my $arow  = shift;
    return "INNER JOIN\n    " . $arow->_table_name .' AS '. $arow->_alias . "\n";
}


sub st_left_outer_join {st_left_join(@_)};
sub st_left_join {
    my $self = shift;
    my $arow  = shift;
    UNIVERSAL::isa($arow, 'SQL::DB::ARow') || confess "join only with ARow";
    push(@{$self->{query}}, 'sql_left_join', $arow);
    return;
}


sub sql_left_join {
    my $self = shift;
    my $arow  = shift;
    return "LEFT OUTER JOIN\n    " . $arow->_table_name .' AS '. $arow->_alias . "\n";
}


sub st_right_outer_join {st_right_join(@_)};
sub st_right_join {
    my $self = shift;
    my $arow  = shift;
    push(@{$self->{query}}, 'sql_right_join', $arow);
    return;
}


sub sql_right_join {
    my $self = shift;
    my $arow  = shift;
    return "RIGHT OUTER JOIN\n    ". $arow->_table_name .' AS '. $arow->_alias ."\n";
}


sub st_full_join {
    my $self = shift;
    my $arow  = shift;
    push(@{$self->{query}}, 'sql_full_join', $arow);
    return;
}
sub st_full_outer_join {st_full_join(@_)};


sub sql_full_join {
    my $self = shift;
    my $arow  = shift;
    return "FULL OUTER JOIN\n    ". $arow->_table_name .' AS '. $arow->_alias ."\n";
}


sub st_cross_join {
    my $self = shift;
    my $arow  = shift;
    push(@{$self->{query}}, 'sql_cross_join', $arow);
    return;
}


sub sql_cross_join {
    my $self = shift;
    my $arow  = shift;
    return "CROSS JOIN\n    ". $arow->_table_name .' AS '. $arow->_alias ."\n";
}



sub st_union {
    my $self = shift;
    my $ref  = shift;
    unless(UNIVERSAL::isa($ref, 'SQL::DB::Expr')) {
        confess "Select UNION must be based on SQL::DB::Expr";
    }
    push(@{$self->{query}}, 'sql_union', $ref);
    $self->push_bind_values($ref->bind_values);
    return;
}


sub sql_union {
    my $self = shift;
    my $ref  = shift;
    return "UNION \n" . $ref . "\n";
}


sub st_group_by {
    my $self = shift;
    my $ref  = shift;
    push(@{$self->{query}}, 'sql_group_by', $ref);
    return;
}


sub sql_group_by {
    my $self = shift;
    my $ref  = shift;

    if (ref($ref) eq 'ARRAY') {
        return "GROUP BY\n    ".
               join(",\n    ", map {$_} @{$ref}) ."\n";
    }
    return "GROUP BY\n    " . $ref . "\n";
}


sub st_order_by {
    my $self = shift;
    my $ref  = shift;
    push(@{$self->{query}}, 'sql_order_by', $ref);
    return;
}


sub sql_order_by {
    my $self = shift;
    my $ref  = shift;

    if (ref($ref) eq 'ARRAY') {
        return "ORDER BY\n    ".
               join(",\n    ", @{$ref}) ."\n";
    }
    return "ORDER BY\n    " . $ref . "\n";
}


sub st_limit {
    my $self = shift;
    my $val  = shift;
    push(@{$self->{query}}, 'sql_limit', $val);
    return;
}


sub sql_limit {
    my $self = shift;
    my $val  = shift;
    return 'LIMIT ' . $val . "\n";
}


sub st_offset {
    my $self = shift;
    my $val  = shift;
    push(@{$self->{query}}, 'sql_offset', $val);
    return;
}


sub sql_offset {
    my $self = shift;
    my $val  = shift;
    return 'OFFSET ' . $val . "\n";
}



# ------------------------------------------------------------------------
# DELETE
# ------------------------------------------------------------------------

sub st_delete {
    my $self = shift;
    my $arow = shift;
    UNIVERSAL::isa($arow, 'SQL::DB::ARow') ||
        confess "Can only delete type SQL::DB::ARow";
    push(@{$self->{query}}, 'sql_delete', $arow);
    return;
}
sub st_delete_from {st_delete(@_)};


sub sql_delete {
    my $self = shift;
    my $arow = shift;
    return "DELETE FROM\n    ". $arow->_table->name ."\n"; 
}


1;


__END__
# vim: set tabstop=4 expandtab:
