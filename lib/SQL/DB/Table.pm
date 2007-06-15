package SQL::DB::Table;
use strict;
use warnings;
use overload '""' => 'sql';
use Carp qw(carp croak confess);
use Scalar::Util qw(weaken);
use SQL::DB::Column;
use SQL::DB::ARow;

        use Data::Dumper;
        $Data::Dumper::Indent = 1;
# FIXME  Take the PRIMARY value from the column and add to a 'primary'
#        method
#
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
        name         => lc($name),
        def          => \%{$column_def},
        schema       => $schema,
    };
    weaken($self->{schema});

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
        not_null
        is_not_null
); 

sub setup {
    my $self = shift;

    $self->{columns}      = undef;
    $self->{column_names} = undef;
    $self->{primary_keys} = undef;
    $self->{bind_values}  = undef;
    $self->{unique}       = undef;
    $self->{foreign}      = undef;

    foreach my $coldef (@{$self->{def}->{columns}}) {
        my $col_name = $coldef->{name};

        if (grep(m/^$col_name$/, @reserved)) {
            croak "Reserved column names: @reserved";
        }

        if ($self->{column_names}->{$col_name}) {
            croak "Column $col_name already defined for table $self->{name}";
        }

        if (exists($coldef->{primary}) and $coldef->{primary}) {
            if ($self->{def}->{primary}) {
                croak "Table $self->{name}: Multiple primary column's "
                     ."must be defined at the table level";
            }
            delete $coldef->{primary};
            $self->{def}->{primary} = $col_name;
        }

        if (exists($coldef->{unique}) and $coldef->{unique}) {
            if (my $unique = $self->{def}->{unique}) {
                delete $coldef->{unique};
                push(@{$unique}, [$col_name]);
            }
            else {
                $self->{def}->{unique} = [[$col_name]];
            }
        }

        my $col = SQL::DB::Column->new(
            table => $self,
            def   => $coldef,
        );
        push(@{$self->{columns}}, $col);
        $self->{column_names}->{$col_name} = $col;

        push(@{$self->{bind_values}}, $col->bind_values);
    }

    if (my $primary = delete $self->{def}->{primary}) {
        $self->{primary_keys} = [$self->text2cols($primary)];
    }

#    foreach my $key (@{$self->{def}->{primary}}) {
#        if (!exists($self->{column_names}->{$key})) {
#            croak "Primary key $key does not exist in table $self->{name}";
#        }
#        push(@{$self->{primary_keys}}, $self->{column_names}->{$key});
#    }

    if (my $unique = delete $self->{def}->{unique}) {
        foreach (@{$unique}) {
            push(@{$self->{unique}}, [$self->text2cols($_)]);
        }
    }

    if (my $foreign = delete $self->{def}->{foreign}) {
        foreach my $f (@{$foreign}) {
            unless (ref($f) and ref($f) eq 'HASH') {
                die "foreign definition must be HASHref";
            }
            push(@{$self->{foreign}},{
                columns    => [$self->text2cols($f->{columns})],
                references => [$self->text2cols($f->{references})],
            });
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
    $self->{engine} = delete $self->{def}->{engine};
    $self->{default_charset} = delete $self->{def}->{default_charset};
    delete $self->{def};
}


sub text2cols {
    my $self = shift;
    my $text = shift;
    my @cols = ();

    if (ref($text) and ref($text) eq 'ARRAY') {
        return map {$self->text2cols($_)} @{$text};
    }

    if (ref($text)) {
        confess "text2cols called with non-scalar and non-arrayref: $text";
    }

    if ($text =~ /\s*(.*)\s*\((.*)\)/) {
        my $table;
        unless (eval {$table = $self->{schema}->table($1);1;}) {
            croak "Table $self->{name}: Foreign table $1 not yet defined";
        }
        foreach my $column_name (split(/,\s*/, $2)) {
            unless($table->column($column_name)) {
                croak "Table $self->{name}: Foreign table '$1' has no "
                     ."column '$column_name'";
            }
            push(@cols, $table->column($column_name));
        }
    }
    else {
        foreach my $column_name (split(/,\s*/, $text)) {
            unless(exists($self->{column_names}->{$column_name})) {
                croak "Table $self->{name}: No such column '$column_name'";
            }
            push(@cols, $self->{column_names}->{$column_name});
        }
    }
    if (!@cols) {
        croak 'No columns found in text: '. $text;
    }
    return @cols;
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


sub primary_keys {
    my $self = shift;
    return @{$self->{primary_keys}};
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

    if (!$self->{unique}) {
        return ();
    }

    my @sql = ();

    # a list of arrays
    foreach my $u (@{$self->{unique}}) {
        push(@sql, 'UNIQUE ('
                . join(', ', map {$_->name} @{$u})
                . ')'
        );
    }

    return @sql;
}


sub foreign_sql {
    my $self = shift;
    if (!$self->{foreign}) {
        return '';
    }
    my $sql = '';
    foreach my $f (@{$self->{foreign}}) {
        my @cols = @{$f->{columns}};
        my @refs = @{$f->{references}};
        $sql .= 'FOREIGN KEY ('
                . join(', ', @cols)
                . ') REFERENCES ' . $refs[0]->table->name .' ('
                . join(', ', @refs)
                . ')'
        ;
    }
    return $sql;
}


sub sql_engine {
    my $self = shift;
    if (!$self->{engine}) {
        return '';
    }
    return ' ENGINE='.$self->{engine};
}

sub sql_default_charset {
    my $self = shift;
    if (!$self->{default_charset}) {
        return '';
    }
    return ' DEFAULT_CHARSET='.$self->{default_charset};
}


sub sql {
    my $self = shift;
    my @vals = $self->columns;
    push(@vals, $self->primary_sql) if ($self->{primary_keys});
    push(@vals, $self->unique_sql) if ($self->{unique});
    push(@vals, $self->foreign_sql) if ($self->{foreign});

    return 'CREATE TABLE '
           . $self->{name}
           . " (\n    " . join(",\n    ", @vals) . "\n)"
           . $self->sql_engine
           . $self->sql_default_charset
    ;
}

sub sql_index {
    my $self = shift;
    my @sql = ();
    foreach my $index (@{$self->{indexes}}) {
        my @cols = @{$index->{columns}};
        my @colsflat;
        foreach (@cols) {
            (my $x = $_) =~ s/\s/_/g;
            push(@colsflat, $x);
        }
        push(@sql, 'CREATE'
                . ($index->{unique} ? ' UNIQUE' : '')
                . ' INDEX '
                . join('_',$self->{name}, @colsflat)
                . " ON $self->{name} ("
                . join(', ', @cols)
                . ')'
                . ($index->{using} ? " USING $index->{using}" : '')
        );
    }
    return @sql;
}


sub bind_values {
    my $self = shift;
    return @{$self->{bind_values}};
}


sub abstract_row {
    my $self = shift;
    return SQL::DB::ARow->_new($self);
}


DESTROY {
    my $self = shift;
    warn "DESTROY $self" if($main::DEBUG);
}


1;
__END__
