package SQL::DB::Table;
use strict;
use warnings;
use overload '""' => 'sql';
use Carp qw(carp croak confess);
use Scalar::Util qw(weaken);
use SQL::DB::Column;
use SQL::DB::ARow;

our $DEBUG;

use Data::Dumper;
$Data::Dumper::Indent = 1;

my @reserved = qw(
        sql
        sql_index
        bind_values
        asc
        desc
        is_null
        not_null
        is_not_null
        exists
); 


sub new {
    my $proto      = shift;
    my $class      = ref($proto) || $proto;

    my $def        = shift;
    my $schema     = shift;

    unless (ref($def)  and ref($def) eq 'ARRAY' and ref($schema)) {
        croak 'usage: new($arrayref, $schema)';
    }

    my $self = {
        schema       => $schema,
    };
    bless($self, $class);
    weaken($self->{schema});


    my @definitions = @{$def};
    while (my $key = shift(@definitions)) {
        my $action = 'setup_'.$key;
        my $val    = shift @definitions;
        if ($self->can($action)) {
            if (ref($val)) {
                $self->$action(@{$val});
            }
            else {
                $self->$action($val);
            }
        }
        else {
            warn "Unknown Table definition: $key", Dumper($key);
            shift @definitions;
        }
    }

    return $self;
}

sub setup_table {
    my $self      = shift;
    $self->{name} = shift;
    if ($self->{name} =~ m/[A-Z]/) {
        warn "Table '$self->{name}' is not all lowercase";
    }
}


sub setup_columns {
    my $self = shift;

    foreach my $array (@_) {
        my $col = SQL::DB::Column->new();
        $col->table($self);

        while (my $key = shift @{$array}) {
            if ($key eq 'name') {
                my $val = shift @{$array};
                if (grep(m/^$val$/, @reserved)) {
                    croak "Column can't be called '$val': reserved name";
                }

                if (exists($self->{column_names}->{$val})) {
                    croak "Column $val already defined for table $self->{name}";
                }
                $col->name($val);
            }
            elsif ($key eq 'references') {
                $col->$key($self->text2cols(shift @{$array}));
            }
            else {
                $col->$key(shift @{$array});
            }
        }
        push(@{$self->{columns}}, $col);
        push(@{$self->{bind_values}}, $col->bind_values);
        $self->{column_names}->{$col->name} = $col;
    }
}


sub setup_primary {
    my $self = shift;
    push(@{$self->{primary}}, $self->text2cols(@_));
}


sub setup_unique {
    my $self = shift;
    foreach my $def (@_) {
        push(@{$self->{unique}}, [$self->text2cols($def)]);
    }
}


sub setup_indexes {
    my $self = shift;
    foreach my $i (@_) {
        my %hash = @{$i};
        foreach my $col (@{$hash{columns}}) {
            (my $c = $col) =~ s/\s.*//;
            if (!exists($self->{column_names}->{$c})) {
                croak "Index column $c does not exist in table $self->{name}";
            }
        }
    }
}


sub setup_foreign {
    my $self = shift;
    warn 'multi foreign not implemented yet';
}


sub setup_type {
    my $self = shift;
    $self->{type} = shift;
}


sub setup_engine {
    my $self = shift;
    $self->{engine} = shift;
}


sub setup_default_charset {
    my $self = shift;
    $self->{default_charset} = shift;
}


sub setup_tablespace {
    my $self = shift;
    $self->{tablespace} = shift;
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
    return @{$self->{primary}};
}


sub schema {
    my $self = shift;
    return $self->{schema};
}


sub primary_sql {
    my $self = shift;
    if (!$self->{primary}) {
        return '';
    }
    return 'PRIMARY KEY('
           . join(', ', map {$_->name} @{$self->{primary}}) .')';
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
    my @vals = map {$_->name} $self->columns;
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
    warn "DESTROY $self" if($DEBUG);
}


1;
__END__
