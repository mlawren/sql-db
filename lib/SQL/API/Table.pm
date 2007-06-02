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
    return;
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
    $self->{columns}      = [];
    $self->{column_names} = {};
    $self->{bind_values}  = [];
    $self->{unique}       = [];
    $self->{foreign}      = [];

    foreach my $coldef (@{$self->{def}->{columns}}) {
        my $col_name = $coldef->{name};
        if ($col_name =~ m/(^sql$)|(^sql_index$)|(^bind_values$)/) {
            croak "SQL::API sql,sql_index,bind_values are reserved column names";
        }
        if ($self->{column_names}->{$col_name}) {
            croak "Column $col_name already defined for table $self->{name}";
        }

        if (exists($coldef->{foreign})) {
            push(@{$self->{foreign}}, {
                columns  => [$col_name],
                table   => $coldef->{foreign}->{table},
                fcolumns => [$coldef->{foreign}->{fcolumn}],
            });
        }

        my $col = SQL::API::Column->new(
            name  => $col_name,
            table => $self,
            def   => $coldef,
        );
        push(@{$self->{columns}}, $col);
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

    foreach my $key (@{$self->{def}->{primary}}) {
        if (!exists($self->{column_names}->{$key})) {
            croak "Primary key $key does not exist in table $self->{name}";
        }
        push(@{$self->{primary_keys}}, $self->{column_names}->{$key});
    }

    if ($self->{def}->{unique}) {
        if (!ref($self->{def}->{unique}->[0])) {
            $self->{unique} = [{columns => $self->{def}->{unique}}];
        }
        else {
            foreach my $u (@{$self->{def}->{unique}}) {
                push(@{$self->{unique}}, $u);
            }
        }
    }

    foreach my $u (@{$self->{unique}}) {
        foreach my $c (@{$u->{columns}}) {
            if (!exists($self->{column_names}->{$c})) {
                croak "Unique column $c does not exist in table $self->{name}";
            }
        }
    }

    if ($self->{def}->{foreign}) {
        foreach my $u (@{$self->{def}->{foreign}}) {
            push(@{$self->{foreign}}, $u);
        }
    }

    foreach my $u (@{$self->{foreign}}) {
        foreach my $c (@{$u->{columns}}) {
            if (!exists($self->{column_names}->{$c})) {
                croak "Foreign column $c does not exist in table $self->{name}";
            }
            # FIXME check that the referenced columns exist in the other Table
        }
    }

    $self->{indexes} = $self->{def}->{indexes} || [];
    foreach my $i (@{$self->{indexes}}) {
        foreach my $col (@{$i->{columns}}) {
            (my $c = $col) =~ s/\s.*//;
            if (!exists($self->{column_names}->{$c})) {
                croak "Index column $c does not exist in table $self->{name}";
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
    return @{$self->{columns}};
}


sub _column_names {
    my $self = shift;
    return keys %{$self->{column_names}};
}


sub _column {
    my $self = shift;
    my $name = shift;
    if (!exists($self->{column_names}->{$name})) {
        return;
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
    return 'PRIMARY KEY('
           . join(', ', map {$_->name} @{$self->{primary_keys}}) .')';
}



sub _unique_sql {
    my $self = shift;
    if (!@{$self->{unique}}) {
        return '';
    }
    my $sql = '';
    foreach my $u (@{$self->{unique}}) {
        my @cols = @{$u->{columns}};
        $sql .= 'UNIQUE ('
                . join(', ', @cols)
                . ')'
        ;
    }
    return $sql;
}


sub _foreign_sql {
    my $self = shift;
    if (!@{$self->{foreign}}) {
        return '';
    }
    my $sql = '';
    foreach my $f (@{$self->{foreign}}) {
        my @cols = @{$f->{columns}};
        my @refs = @{$f->{fcolumns}};
        $sql .= 'FOREIGN KEY ('
                . join(', ', @cols)
                . ") REFERENCES $f->{table} ("
                . join(', ', @refs)
                . ')'
        ;
    }
    return $sql;
}


sub sql {
    my $self = shift;
    return 'CREATE TABLE '.$self->{name}. " (\n    "
           . join(",\n    ",
                $self->_columns,
                $self->_primary_sql,
                $self->_unique_sql,
                $self->_foreign_sql,
           )
           . "\n)"
    ;
}

sub sql_index {
    my $self = shift;
    my $sql = '';
    foreach my $index (@{$self->{indexes}}) {
        my @cols = @{$index->{columns}};
        my @colsflat;
        foreach (@cols) {
            (my $x = $_) =~ s/\s/_/g;
            push(@colsflat, $x);
        }
        $sql .= 'CREATE'
                . ($index->{unique} ? ' UNIQUE' : '')
                . ' INDEX '
                . join('_',$self->{name}, @colsflat)
                . " ON $self->{name} ("
                . join(', ', @cols)
                . ')'
                . ($index->{using} ? " USING $index->{using}" : '')
        ;
    }
    return $sql;
}


sub bind_values {
    my $self = shift;
    return @{$self->{bind_values}};
}

1;
__END__
