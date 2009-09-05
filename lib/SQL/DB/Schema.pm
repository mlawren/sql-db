package SQL::DB::Schema;
use Mouse;
use Carp qw/croak carp/;
use SQL::DB::Table;
use SQL::DB::Column;
use SQL::DB::Index;
use SQL::DB::FK;
use SQL::DB::Query;

our $VERSION = '0.18';


has 'name' => (
    required => 1,
    is => 'ro',
    isa => 'Str',
);

has 'seqtable' => (
    is => 'ro',
    isa => 'Str',
    lazy => 1,
    builder => sub {
        ( my $n = lc( shift->name ) ) =~ s/[^0-9a-zA-Z]/_/;
        return $n .'_sequences';
    },
);

has 'debug' => (
    is => 'rw',
    isa => 'Bool',
    default => 0,
);

has 'dbd' => (
    is => 'rw',
    isa => 'Str',
    init_arg => undef,
);

has '_tables' => (
    is => 'ro',
    isa => 'HashRef[SQL::DB::Table]',
    default => sub { {} },
    init_arg => undef,
    auto_deref => 1,
);

has '_fk_resolved' => (
    is => 'rw',
    isa => 'Bool',
    default => 0,
    init_arg => undef,
);


sub BUILD {
    my $self = shift;
    $self->define_table(
        name => $self->seqtable,
        columns => [
            { name => 'name', type => 'varchar(32)', primary => 1 },
            { name => 'inc', type => 'int', default => 1 },
            { name => 'val', type => 'int', default => 0 },
        ],
    );
}


# subroutine not a method.
sub _text2cols {
    my $cols = shift;
    my $text = shift;

    my @cols;

    $text =~ s/(^\s)|(\s$)//;
    foreach my $name (split(/\s*,\s*/, $text)) {
        unless( exists $cols->{$name} ) {
            croak "No such column '$name' "
                .".\nKnown Columns: ". join( ',', keys %$cols ) ;
        }
        push( @cols, $cols->{$name} );

    }

    if (!@cols) {
        confess "No columns found in '$text'";
    }
    return @cols;
}


sub _text2fcols {
    my $self = shift;
    my $text = shift;
    my @cols = ();

    if ($text =~ /\s*(.*)\s*\(\s*(.*)\s*\)/) {
        my $table;
        unless ( eval { $table = $self->table( $1 ) } ) {
            confess  ": Foreign table $1 not yet defined.\n"
                . "Known tables: "
                . join(',', $self->tables);
        }
        foreach my $column_name (split(/\s*,\s*/, $2)) {
            unless( eval { $table->column($column_name) } ) {
                confess "Foreign table $1 has no column '$column_name'";
            }
            push( @cols, $table->column( $column_name ) );
        }
    }

    if (!@cols) {
        confess "'$text' is not a valid reference";
    }
    return @cols;
}


sub resolve_fk {
    my $self = shift;
    return 1 if ($self->_fk_resolved);

    foreach my $table ( $self->tables ) {
        foreach my $ref ( $table->_foreign ) {
            my @cols = @{ $ref->{columns} };
            my @references = $self->_text2fcols( $ref->{references} );
            $ref->{references} = \@references;

            if (@cols != @references) {
                croak 'Unmatched/missing reference columns for '
                    . $table->name .'('. join(',', map {$_->name} @cols) .')';
            }

            my $i = -1;
            while ($i++ < $#cols) {
                $cols[$i]->references( $references[$i] );
            }

            push ( @{ $table->foreign }, SQL::DB::FK->new( $ref ) );
        }
        $table->_foreign([]);
    }

    $self->_fk_resolved(1);
}


sub define_table {
    my $self = shift;

#    local %Carp::Internal;
#    $Carp::Internal{'SQL::DB'}++;

    my $ref; # table reference

    if ( ref $_[0] eq 'ARRAY' ) {
        return map { $self->define_table( @$_ ) } @_;
    }
    elsif ( $_[0] eq 'name' ) {
        $ref = { @_ };
    }
    else {
        $ref = shift;
    }

    $ref->{schema} = $self;
    $ref->{name} || confess "Missing table 'name' definition";

    if ( exists $self->_tables->{ $ref->{name} } ) {
        warn "Redefining table '". $ref->{name}
            ."' in schema '".  $self->name ."'";
    }

    # Save for later
    my $unique    = delete $ref->{unique};
    my $primary   = delete $ref->{primary};
    my $sequences = delete $ref->{sequences};
    my $indexes   = delete $ref->{indexes};
    my $foreign   = delete $ref->{foreign};

    # build the columns
    my @columns;
    my $_columns = {};

    foreach my $colref ( @{ $ref->{columns} } ) {

        if ( ref $colref eq 'ARRAY' ) {
            $colref = { @$colref };
        }

        if ( exists $colref->{ref} ) {
            croak "'ref' is depreciated in favour of 'references'";
        }

        $colref->{_references} = delete $colref->{references};
        if ( $colref->{_references} ) {
            push( @{ $foreign },
                {
                    columns => $colref->{name},
                    references => $colref->{_references}
                } 
            );
        }

        my $col = SQL::DB::Column->new( $colref );

        push( @columns, $col );
        $_columns->{ $col->name } = $col;

        if ( $col->unique ) {
            push( @{ $ref->{unique} }, [ $col ] );
        }

        if ( $col->primary ) {
            push( @{ $ref->{primary} }, $col );
            $ref->{_primary}->{$col->name} = $col;
        }
    }

    $ref->{_columns} = $_columns;
    $ref->{columns}  = \@columns;

    if ( $primary ) {
        my @cols = _text2cols( $_columns, $primary );
        push( @{ $ref->{primary} }, @cols );
        map {
            $_->primary( 1 );
            $ref->{_primary}->{$_->name} = $_;
        } @cols;
    }

    if ( $unique ) {
        my @unique = map {
            ref $_ eq 'HASH'
                ? [ _text2cols( $_columns, $_->{columns} ) ]
                : [ _text2cols( $_columns, $_ ) ]
        } @$unique;
        push( @{ $ref->{unique} }, @unique );
    }

    # Everything else need the columns to be attached to a table
    my $table = SQL::DB::Table->new( $ref );
    map { $_->table( $table ) } @columns;

#    $table->aclass( SQL::DB::ARow->make_class_from( @columns ) );
#    $table->class( SQL::DB::Row->make_class_from( @columns ) );

    if ( $indexes ) {
        my @indexes = map {
            my $idx = ref $_ eq 'HASH' ? $_ : { columns => $_ };
            $idx->{columns} = [ _text2cols( $_columns, $idx->{columns} ) ];
            $idx->{table} = $table;
            SQL::DB::Index->new( $idx );
        } @$indexes;
        $table->indexes( \@indexes );
    }

    if ( $sequences ) {
        my @sequences = map {
            $_->{table} = $table;
            SQL::DB::Sequences->new( $_ );
        } @$sequences;
        $table->sequences( \@sequences );
    }

    if ( $foreign ) {
        my @foreign = map {
            $_->{columns} ||= delete $_->{key};
            $_->{columns} = [ _text2cols( $_columns, $_->{columns} ) ];
            $_;
        } @$foreign;
        $table->_foreign( \@foreign );
    }

    $self->_tables->{ $table->name } = $table;
    $self->_fk_resolved(0);

    warn 'debug: defined '. $self->name .'.'. $table->name if ( $self->debug );
    return $table;
}


sub tables {
    my $self = shift;
    return values %{ $self->_tables };
}


sub table {
    my $self = shift;
    my $name  = shift || croak 'usage: table($name)';

    if (!exists($self->_tables->{$name})) {
#        carp "Table '$name' does not exist. "
#            . "Known tables: ". join(', ', keys %{ $self->_tables } );
        return;
    }
    return $self->_tables->{$name};
}


# calculate which tables reference which other tables, and plan the
# deployment order accordingly.
sub deploy_order {
    my $self     = shift;
    my @src      = grep { $_->name ne $self->seqtable } $self->tables;
    my $deployed = {};
    my @ordered  = ();
    my $count    = 0;
    my $limit    = scalar @src + 10;

    while (@src) {
        if ($count++ > $limit) {
            die 'Deployment calculation limit exceeded: circular foreign keys?';
        }

        my @newsrc = ();
        foreach my $table (@src) {
            my $deployable = 1;
            foreach my $c ($table->columns) {
                if (my $foreignc = $c->references) {
                    if ($foreignc->table == $table or # self reference
                        $deployed->{$foreignc->table->name}) {
                        next;
                    }
                    $deployable = 0;
                }
            }

            if ($deployable) {
#                warn "debug: ".$table->name.' => deploy list ' if($self->{sqldb_sqldebug});
                push(@ordered, $table);
                $deployed->{$table->name} = 1;
            }
            else {
                push(@newsrc, $table);
            }
        }
        @src = @newsrc;

    }
    return $self->table( $self->seqtable ), @ordered;
}


sub as_sql {
    my $self = shift;
    unless ( $self->_fk_resolved ) {
        croak "Cannot generate schema SQL unless foreign keys resolved";
    }

    if ( wantarray ) {
        return map { $_->as_sql } $self->deploy_order;
    }

    return join(";\n", map { $_->as_sql } $self->deploy_order ). ";\n";
}


sub row {
    my $self = shift;
    my $tname = shift || 'Unknown';
    my $table = $self->table( $tname ) || confess "no such table: $tname";
    return $table->row( @_ );
}


sub arow {
    my $self  = shift;
    my $table = $self->table( $_[0] ) || confess "No such table: $_[0]";
    return $table->arow;
}


sub arows {
    my $self = shift;
    return map { $self->arow( $_ ) } @_;
}


sub query {
    my $self = shift;
    local %Carp::Internal;
    local %Carp::CarpInternal;
    $Carp::Internal{'SQL::DB::Schema'}++;
    $Carp::CarpInternal{'SQL::DB::Schema'}++;

    return SQL::DB::Query->new( @_ );
}


1;
__END__

=head1 NAME

SQL::DB::Schema - Database schema model for SQL::DB

=head1 VERSION

0.18. Development release.

=head1 SYNOPSIS

  use SQL::DB::Schema;
  my $schema = SQL::DB::Schema->new( name => 'myschema' );

  $schema->define_table(
      table => $name,
      columns => [
          [ name => 'id', type => 'int', primary => 1 ],
          [ name => 'id', type => 'int', primary => 1 ],
          [ name => 'id', type => 'int', primary => 1 ],
      ],
      indexes => [
          [ columns => '', unique => 1, using => 'btree' ],
      ],
      foreign_keys => [
           [ columns => 'col1,col2' => reference => 'remote(col1,col2)'],
      ],
  );

  $schema->resolve_fk();

  # To see the different flavours of SQL
  foreach my $dbd (qw/Pg SQLite mysql/) {
      $schema->dbd($dbd);
      print $schema->as_sql;
  }

  foreach my $t ($schema->tables) {
      # Do something with the SQL::DB::Table object
      print $t->foreign->[0]->table->primary->[0]->name;
  }


=head1 DESCRIPTION

L<SQL::DB> provides a low-level interface to SQL databases, using Perl
objects and logic operators. B<SQL::DB::Schema> is used by L<SQL::DB>
as a container for L<SQL::DB::Table> objects. Most users will only
use this module indirectly through L<SQL::DB>.

=head1 CONSTRUCTOR

The new() constructor requires the 'name' attribute.

=head1 ATTRIBUTES

=over 4

=item name

The name of this schema.

=item seqtable

The name of sequence table for this schema. Only used by DBDs that
don't have native sequence support.

=item debug <-> Bool

General debugging state (true/false). Debug messages are 'warn'ed.

=item dbd

The DBD driver that the schema should map to. This affects the way
tables and columns present themselves.

=item tables

A list of L<SQL::DB::Table> objects in the schema.

=back

=head1 METHODS

=over 4

=item define(@table_definition)

(Re)Define a table in the schema.

=item table($name) -> SQL::DB::Table

Returns the table $name if it exists, undef otherwise.

=item resolve_fk

Resolves the foreign key relationships, making sure that each foreign
key refers to a known column in the schema.  It is called automatically
when the L<SQL::DB> 'schema' attribute is set.

=item as_sql -> @list | $str

In array context returns a list of SQL CREATE TABLE/INDEX/TRIGGER
statements.  In scalar context returns a string containing the same
list of statements joined together by ";\n";

=item row( $name, @args ) -> SQL::DB::Row::$name

Returns a new object representing a single row of the table $name, with
values as specified by @args.  Such an object can be inserted (or
potentially updated or deleted) by SQL::DB.

=item arow( $name ) -> SQL::DB::ARow::$name

Returns an object representing I<any> row of table $name. This
abstraction object is used with the SQL::DB 'fetch' and 'do' methods.

=item arows( @names ) -> @{ SQL::DB::ARow::$name1, ... }

Returns objects representing any row of a table. These abstraction
objects are used with the SQL::DB 'fetch' and 'do' methods.

=back

=head1 SEE ALSO

L<SQL::DB::Table>, L<SQL::DB>

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright 2007-2009 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.

=cut

# vim: set tabstop=4 expandtab:

