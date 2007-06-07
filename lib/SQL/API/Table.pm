package SQL::API::Table;
use strict;
use warnings;
use overload '""' => 'sql';
use Carp qw(carp croak);
use SQL::API::Column;
use SQL::API::ARow;

        use Data::Dumper;
        $Data::Dumper::Indent = 1;

sub new {
    my $proto      = shift;
    my $class      = ref($proto) || $proto;

    my $name       = shift;
    my $column_def = shift;
    my $schema     = shift;

    unless ($name and ref($column_def)  and ref($column_def) eq 'HASH') {
        croak 'usage: new($name, $hashref)';
    }

    my $self = {
        name         => $name,
        def          => \%{$column_def},
        schema       => $schema,
    };
    bless($self, $class);

    $self->setup;
    return $self;
}

my @reserved = qw(
        sql
        sql_index
        bind_values
        asc
        desc
        is_null
        is_not_null
); 

sub setup {
    my $self = shift;

    $self->{columns}      = [];
    $self->{column_names} = {};
    $self->{bind_values}  = [];
    $self->{unique}       = [];
    $self->{foreign}      = [];

    foreach my $coldef (@{$self->{def}->{columns}}) {
        my $col_name = $coldef->{name};

        if (grep(m/^$col_name$/, @reserved)) {
            croak "Reserved column names: @reserved";
        }

        if ($self->{column_names}->{$col_name}) {
            croak "Column $col_name already defined for table $self->{name}";
        }

        # move the foreign key definition into the table-wide definitions
        if (exists($coldef->{foreign})) {
            push(@{$self->{foreign}}, {
                columns    => [$col_name],
                references => [delete $coldef->{foreign}],
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

    foreach my $f (@{$self->{foreign}}) {
        # check columns exist in this table
        foreach my $c (@{$f->{columns}}) {
            if (!exists($self->{column_names}->{$c})) {
                croak "Column $c [foreign definition] does not exist "
                     ."in table $self->{name}";
            }
        }

        # check reference columns exist in foreign table
        my @references;
        my $i = 0;

        foreach my $r (@{$f->{references}}) {
            unless ($r =~ m/(.*)\((.*)\)/) {
                croak "Table $self->{name}: Bad foreign key reference: $r";
            }

            my $table;
            my @column_names  = split(/,\s*/, $2);

            unless (eval {$table = $self->{schema}->table($1);1;}) {
                croak "Table $self->{name}: Foreign table $1 not yet "
                     ."defined for foreign keys";
            }

            my @cols;
            foreach my $column_name (@column_names) {
                unless($table->column($column_name)) {
                    croak "Table $self->{name}: Foreign table '$1' has no "
                         ."column '$column_name'";
                }
                push(@cols, $table->column($column_name));
            }
            push(@references, \@cols);
            if (scalar(@cols) == 1) {
                $self->{column_names}->{$f->{columns}->[$i]}->foreign_key(@cols);
            }
        }
        $f->{references} = \@references;
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


sub name {
    my $self = shift;
    return $self->{name};
}


sub columns {
    my $self = shift;
    return @{$self->{columns}};
}


sub column_names {
    my $self = shift;
    return keys %{$self->{column_names}};
}


sub column {
    my $self = shift;
    my $name = shift;
    if (!exists($self->{column_names}->{$name})) {
        return;
    }
    return $self->{column_names}->{$name};
}


sub schema {
    my $self = shift;
    return $self->{schema};
}

sub primary_sql {
    my $self = shift;
    if (!$self->{primary_keys}) {
        return '';
    }
    return 'PRIMARY KEY('
           . join(', ', map {$_->name} @{$self->{primary_keys}}) .')';
}



sub unique_sql {
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


sub foreign_sql {
    my $self = shift;
    if (!@{$self->{foreign}}) {
        return '';
    }
    my $sql = '';
    foreach my $f (@{$self->{foreign}}) {
        my @cols = @{$f->{columns}};
        my @refs = @{$f->{references}};
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
                $self->columns,
                $self->primary_sql,
                $self->unique_sql,
                $self->foreign_sql,
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


sub abstract_row {
    my $self = shift;
    return SQL::API::ARow->_new($self);
}


1;
__END__
