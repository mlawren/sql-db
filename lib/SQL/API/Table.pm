package SQL::API::Table;
use strict;
use warnings;
use overload '""' => 'sql';
use Carp qw(carp croak);
use SQL::API::Column;


our %tables;

sub _table {
    my $proto = shift;
    my $name  = shift;
    if (exists($tables{$name})) {
        return $tables{$name};
    }
    croak "Table $name has not been defined";
}


sub _define_table {
    my $proto = shift;

    my $name       = shift;
    my $column_def = shift;

    if (!$name or ref($column_def) ne 'HASH') {
        croak 'usage: _define_table($name, $hashref)';
    }

    if (exists($tables{$name})) {
        carp "Redefining table $name";
    }

    no strict 'refs';
    my $pkg = 'SQL::API::Table::'. $name;
    my $isa = $pkg . '::ISA';
    push(@{*{$isa}}, 'SQL::API::Table');

    my $table = $pkg->_new($name,$column_def);
    $tables{$name} = $table;
    return $table;
}


sub _new {
    my $proto      = shift;
    my $class      = ref($proto) || $proto;
    my $name       = shift;
    my $column_def = shift;

    if (!$name or ref($column_def) ne 'HASH') {
        croak 'usage: new($name, $hashref)';
    }

    my $self = {
        name         => $name,
        def          => $column_def,
    };
    bless($self, $class);

    $self->_setup;
    $tables{$name} = $self;
    return $self;
}


sub _setup {
    my $self = shift;
    my $pkg  = ref($self);
    $self->{column_names} = {};
    $self->{bind_values}  = [];

    foreach my $col_name (keys %{$self->{def}->{columns}}) {
        if ($col_name eq 'sql' or $col_name eq 'bind_values') {
            croak "SQL::API reserved column names: sql,bind_values";
        }
        if ($self->{column_names}->{$col_name}) {
            croak "Column $col_name already defined for table $self->{name}";
        }
        my $col = SQL::API::Column->new(
            name  => $col_name,
            table => $self,
            def   => $self->{def}->{columns}->{$col_name},
        );
        $self->{column_names}->{$col_name} = $col;
        push(@{$self->{bind_values}}, $col->bind_values);

        # set up the method in this class
        no strict 'refs';
        my $ref = $pkg."::$col_name";
        *{$ref} = sub {
            my $self = shift;
            return $self->{column_names}->{$col_name};
        }
    }

    if (ref($self->{def}->{primary}) eq 'ARRAY') {
        foreach my $key (@{$self->{def}->{primary}}) {
            if (!exists($self->{column_names}->{$key})) {
                croak "Primary key $key does not exist in table $self->{name}";
            }
            push(@{$self->{primary_keys}}, $self->{column_names}->{$key});
        }
    }
    else {
        if (!exists($self->{column_names}->{$self->{def}->{primary}})) {
            croak 'Primary key '
                  . $self->{def}->{primary}
                  . ' does not exist in table '
                  . $self->{name};
        }
        push(@{$self->{primary_keys}},
              $self->{column_names}->{$self->{def}->{primary}});
    }

    $self->{indexes} = $self->{def}->{indexes} || [];
    foreach my $i (@{$self->{indexes}}) {
        foreach my $col (@{$i->{columns}}) {
            (my $c = $col) =~ s/\s.*//;
            if (!exists($self->{column_names}->{$c})) {
                croak "Index key $c does not exist in table $self->{name}";
            }
        }
    }
    delete $self->{def};
}


sub _name {
    my $self = shift;
    return $self->{name};
}


sub _columns {
    my $self = shift;
    my @names = sort keys %{$self->{column_names}};
    return map {$self->{column_names}->{$_}} @names;
}


sub _column_names {
    my $self = shift;
    return sort keys %{$self->{column_names}};
}


sub _column {
    my $self = shift;
    my $name = shift;
    if (!exists($self->{column_names}->{$name})) {
        croak "Column $name not in table $self->{name}";
    }
    return $self->{column_names}->{$name};
}


sub AUTOLOAD {
    our ($AUTOLOAD);
    (my $colname = $AUTOLOAD) =~ s/.*:://;
    if (ref($_[0])) {
        croak "Table $_[0]->{name} doesn't have a column '$colname'";
    }
    croak "Method $colname not found in package ".__PACKAGE__;
}

sub _primary_sql {
    my $self = shift;
    if (!$self->{primary_keys}) {
        return '';
    }
    return "\n    PRIMARY KEY("
           . join(", ", map {$_->name} @{$self->{primary_keys}}) .")";
}


sub _index_sql {
    my $self = shift;
    my $sql = '';
    foreach my $index (@{$self->{indexes}}) {
        my @cols = @{$index->{columns}};
        my @colsflat;
        foreach (@cols) {
            (my $x = $_) =~ s/\s/_/g;
            push(@colsflat, $x);
        }
        $sql .= "\nCREATE INDEX "
                . join('_',$self->{name}, @colsflat)
                . " ON $self->{name} ("
                . join(', ', @cols)
                . ')'
                . ($index->{using} ? " USING $index->{using}" : '')
        ;
    }
    return $sql;
}


sub sql {
    my $self = shift;
    return 'CREATE TABLE '.$self->{name}. " (\n    "
           . join(",\n    ",$self->_columns)
           . $self->_primary_sql
           . "\n)"
           . $self->_index_sql
    ;
}


sub bind_values {
    my $self = shift;
    return @{$self->{bind_values}};
}

1;
__END__
