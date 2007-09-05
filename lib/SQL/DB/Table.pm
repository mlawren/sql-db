package SQL::DB::Table;
use strict;
use warnings;
use overload '""' => 'sql';
use Carp qw(carp croak confess);
use Scalar::Util qw(weaken);
use SQL::DB::Column;
use SQL::DB::Object;
use SQL::DB::ARow;

our $DEBUG;

my @reserved = qw(
    sql
    sql_index
    asc
    desc
    is_null
    not_null
    is_not_null
    exists
); 


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = {};
    bless($self, $class);

    while (my ($key,$val) = splice(@_, 0, 2)) {
        my $action = 'setup_'.$key;
        if (!$self->can($action)) {
            warn "Unknown Table definition: $key";
            next;
        }

        if (ref($val) and ref($val) eq 'ARRAY') {
            $self->$action(@{$val});
        }
        else {
            $self->$action($val);
        }
    }


    if (my $class = $self->{class}) {
        no strict 'refs';
        my $isa = \@{$class . '::ISA'};
        if (defined @{$isa}) {
            carp "redefining $class";
        }

        my @bases = $self->{bases} ? @{$self->{bases}} : ('SQL::DB::Object');
        push(@{$isa}, @bases);
        $class->mk_accessors($self->column_names);
        ${$class .'::TABLE'} = $self;

        my $aclass = $class . '::Abstract';
        $isa = \@{$aclass . '::ISA'};
        if (defined @{$isa}) {
            carp "redefining $aclass";
        }
        push(@{$isa}, 'SQL::DB::ARow');
        $aclass->mk_accessors($self->column_names);
        ${$aclass .'::TABLE'} = $self;
    }

    return $self;
}


sub setup_schema {
    my $self = shift;
    $self->{schema} = shift;
    weaken($self->{schema});
    return;
}


sub setup_table {
    my $self      = shift;
    $self->{name} = shift;
    if ($self->{name} =~ m/[A-Z]/) {
        warn "Table '$self->{name}' is not all lowercase";
    }
}


sub setup_class {
    my $self       = shift;
    $self->{class} = shift;
}


sub setup_bases {
    my $self       = shift;
    foreach my $class (@_) {
        if (!eval "require $class;1;") {
            die "Base Class $class could not be loaded: $@";
        }
    }
    $self->{bases} = [@_];
}


sub setup_column {
    my $self = shift;

    my $col = SQL::DB::Column->new();
    $col->table($self);

    while (my $key = shift) {
        if ($key eq 'name') {
            my $val = shift;
            if (grep(m/^$val$/, @reserved)) {
                croak "Column can't be called '$val': reserved name";
            }

            if (exists($self->{column_names}->{$val})) {
                croak "Column $val already defined for table $self->{name}";
            }
            $col->name($val);
        }
        else {
            $col->$key(shift);
        }
    }
    push(@{$self->{columns}}, $col);
    $self->{column_names}->{$col->name} = $col;
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
            else {
                $col->$key(shift @{$array});
            }
        }
        push(@{$self->{columns}}, $col);
        $self->{column_names}->{$col->name} = $col;
    }

}


sub setup_primary {
    my $self = shift;
    my $def  = shift;
    push(@{$self->{primary}}, $self->text2cols($def));
}


sub add_primary {
    my $self = shift;
    push(@{$self->{primary}}, @_);
}


sub setup_unique {
    my $self = shift;
    my $def  = shift;
    push(@{$self->{unique}}, [$self->text2cols($def)]);
}


sub setup_index {
    my $self = shift;
    my $hashref = {};

    while (my $def = shift) {
        my $val = shift;
        if ($val) {
            if ($def eq 'columns' and ref($val) and ref($val) eq 'ARRAY') {
                foreach my $col (@{$val}) {
                    (my $c = $col) =~ s/\s.*//;
                if (!exists($self->{column_names}->{$c})) {
                        croak "Index column $c not in table $self->{name}";
                    }
                }
            }
            elsif ($def eq 'columns') {
                my @vals;
                foreach my $col (split(m/,\s*/, $val)) {
                    (my $c = $col) =~ s/\s.*//;
                    if (!exists($self->{column_names}->{$c})) {
                        croak "Index column $c not in table $self->{name}";
                    }
                    push(@vals, $col);
                }
                $val = \@vals;
            }
            $hashref->{$def} = $val;
        }
        else {
            $hashref->{columns} = [$def];
        }
    }
    push(@{$self->{index}}, $hashref);
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
            croak "Table $self->{name}: Foreign table $1 not yet defined.\n".
                  "Known tables: " 
                    . join(',', map {$_->name} $self->{schema}->tables);
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


sub class {
    my $self = shift;
    return $self->{class};
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


sub primary_columns {
    my $self = shift;
    return @{$self->{primary}} if($self->{primary});
    return;
}


sub primary_column_names {
    my $self = shift;
    return map {$_->name} @{$self->{primary}} if($self->{primary});
    return;
}


sub schema {
    my $self = shift;
    return $self->{schema};
}


sub sql_primary {
    my $self = shift;
    if (!$self->{primary}) {
        return '';
    }
    return 'PRIMARY KEY('
           . join(', ', map {$_->name} @{$self->{primary}}) .')';
}


sub sql_unique {
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


sub sql_foreign {
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
    my @vals = map {$_->sql} $self->columns;
    push(@vals, $self->sql_primary) if ($self->{primary});
    push(@vals, $self->sql_unique) if ($self->{unique});
    push(@vals, $self->sql_foreign) if ($self->{foreign});

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

    foreach my $index (@{$self->{index}}) {
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
                . ' ON ' . $self->{name}
                . ($index->{using} ? ' USING '.$index->{using} : '')
                . ' (' . join(', ', @cols) . ')'
        );
    }
    return @sql;
}


DESTROY {
    my $self = shift;
    warn "DESTROY $self" if($DEBUG and $DEBUG>2);
}


1;
__END__

=head1 NAME

SQL::DB::Table - Perl representation of an SQL database table

=head1 SYNOPSIS

  use SQL::DB::Table;

  my $table = SQL::DB::Table->new(
      table   => 'users',
      class   => 'User',
      bases   => [qw(SQL::DB::Object)],
      columns => [
           [name => 'id',  type => 'INT',          primary => 1],
           [name => 'name',type => 'VARCHAR(255)', unique  => 1],
      ],
      index => [
        columns => 'name',
        type    => 'BTREE',
      ],
  );

  print $table->sql;

  #

=head1 DESCRIPTION

B<SQL::DB::Table> objects represent SQL database tables. Once
defined, a B<SQL::DB::Table> object can be queried for information
about the table such as the primary keys, name and type of the
columns, and the SQL table creation syntax.

=head1 DEFINITION KEYS

Key/value pairs can be set multiple times, for example when there is
more than one index in the table.

=head2 schema => $schema

$schema must be a L<SQL::DB::Schema> object. The internal reference to
the schema is set to be weak.

=head2 table => $name

$name is the SQL name of the table.

=head2 class => $name

$name is the Perl class to be created for representing table rows.

=head2 bases => [$class1, $class2,...]

A list of classes that the class will inherit from.

=head2 columns => [ $col1, $col2, ... ]

$col1, $col2, ... are passed directly to L<SQL::DB::Column> new().

=head2 primary => [ $name1, $name2, ... ]

$name1, $name2, ... are the columns names which are primary.
Should only be used if the table has a multiple-column primary key.
If the table has only a single primary key then that should be set
in the column definition.

=head2 unique => [ $name1, $name2, ... ]

$name1, $name2, ... are columns names which must be unique.
Should only be used if the table has a multiple-column unique requirements,
Note that column definitions can also include unique requirements.
This key can be defined more than once with a culmative result.

=head2 index => $def

$def is an array reference of the following form. Note that not all
databases accept all definitions.

  [ columns => 'col1,col2', type => $type ]

=head2 foreign

For multiple foreign key definition. Not presently implemented.

=head2 type => $type

$type specifies the SQL table type. Applies only to PostgreSQL.

=head2 engine => $engine

$engine specifies the SQL backend engine. Applies only to MySQL.

=head2 default_charset => $charset

$charset specifies the SQL default character set. Applies only to MySQL.

=head2 tablespace => $tspace

$tspace specifies the PostgreSQL tablespace definition.

=head1 METHODS

=head2 new(@definition)

Returns a new B<SQL::DB::Table> object. The @definition is a list
of key/value pairs as defined under L<DEFINITION KEYS>.

=head2 name

Returns the SQL name of the database table.

=head2 class

Returns the name of the Perl class which can represent rows in the
table.

=head2 columns

Returns the list of L<SQL::DB::Column> objects representing each column
definition in the database. The order is the same as they were defined.

=head2 column($name)

Returns the L<SQL::DB::Column> object for the column $name.

=head2 column_names

Returns a list of the SQL names of the columns.

=head2 primary_columns

Returns the list of L<SQL::DB::Column> objects which have been defined
as primary.

=head2 primary_column_names

Returns the list of columns names which have been defined as primary.

=head2 schema

Returns the schema (a L<SQL::DB::Schema> object) which this table
is a part of.

=head2 sql

Returns the SQL statement for table creation.

=head2 sql_index

Returns the list of SQL statements for table index creation.

=head1 INTERNAL METHODS

These are used internally but are documented here for completeness.

=head2 add_primary

=head2 text2cols

=head1 SEE ALSO

L<SQL::DB::Schema>, L<SQL::DB::Column>, L<SQL::DB>

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

=cut

# vim: set tabstop=4 expandtab:
