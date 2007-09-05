package SQL::DB;
use 5.006;
use strict;
use warnings;
use Carp qw(carp croak confess);
use DBI;
use Scalar::Util qw(refaddr);
use UNIVERSAL;
use SQL::DB::Schema;
use Class::Accessor::Fast;

use Data::Dumper;
$Data::Dumper::Indent = 1;

our $VERSION = '0.04';
our $DEBUG   = 0;


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = bless({}, $class);
    $self->{schema} = SQL::DB::Schema->new(@_);
    return $self;
}


sub define {
    my $self = shift;
    $self->{schema}->define(@_);
}


sub schema {
    my $self = shift;
    if (@_) {
        my $schema = shift;
        UNIVERSAL::isa($schema, 'SQL::DB::Schema') ||
            croak "Schema must be an SQL::DB::Schema object";
        $self->{schema} = $schema;
    }
    return $self->{schema};
}


sub connect_cached {
    my $self = shift;
    $self->{_connect} = 'connect_cached';
    return $self->connect(@_);
}


sub connect {
    my $self = shift;
    my $method = $self->{_connect} || 'connect';

    my ($dbi,$user,$pass,$attrs) = @_;

    if (my $dbh = DBI->$method($dbi,$user,$pass,$attrs)) {
        $self->{dbh} = $dbh;
    }
    else {
        croak $DBI::errstr;
    }

    $self->{dbi}    = $dbi,
    $self->{user}   = $user,
    $self->{pass}   = $pass,
    $self->{attrs}  = $attrs,
    $self->{qcount} = 0,

    warn "debug: $method to $dbi" if($DEBUG);
    return;
}


sub dbh {
    my $self = shift;
    return $self->{dbh};
}


sub deploy {
    my $self = shift;

    foreach my $table ($self->{schema}->tables) {
        my $sth = $self->{dbh}->table_info('', '', $table->name, 'TABLE');
        if (!$sth) {
            die $DBI::errstr;
        }

        if ($sth->fetch) {
            carp 'Table '. $table->name .' already exists - not deploying';
            next;
        }

        foreach my $action ($table->sql, $table->sql_index) {
            warn "debug: $action" if($DEBUG);
            if (!$self->{dbh}->do($action)) {
                die $self->{dbh}->errstr;
            }
        }
    }
}


# ------------------------------------------------------------------------
# For everything other than SELECT
# ------------------------------------------------------------------------

sub do {
    my $self  = shift;
    my $query = $self->{schema}->query(@_);
    my ($sql,$attrs,@bind) = ($query->sql, undef, $query->bind_values);

    my $rv;
    eval {
        $rv = $self->{dbh}->do($sql, $attrs, @bind);
    };
    if ($@ or !defined($rv)) {
        croak "DBI::do $DBI::errstr $@: Query was:\n"
              . "$sql/* ". join(', ', map {"'$_'"} @bind) . " */\n";
    }
    $self->{qcount}++;

    carp "debug: $sql/* ".  join(', ',map {defined($_) ? "'$_'" : 'NULL'}
                                 @bind) ." */ RESULT: $rv" if($DEBUG);
    return $rv;
}


sub execute {
    my $self = shift;
    my ($sql,$attrs,@bind) = @_;

    my $sth;
    eval {
        $sth = $self->{dbh}->prepare($sql);
    };
    if ($@ or !$sth) {
        croak "DBI::prepare $DBI::errstr $@: Query was:\n"
              . "$sql/* ". join(', ', map {"'$_'"} @bind) . " */\n";
    }

    my $res;
    eval {
        $res = $sth->execute(@bind);
    };
    if (!$res or $@) {
        croak "DBI::execute $DBI::errstr $@: Query was:\n"
              . "$sql/* ". join(', ', map {"'$_'"} @bind) . " */\n";
    }

    carp "debug: $sql/* ". join(', ',map {defined($_) ? "'$_'" : 'NULL'}
                                @bind) ." */ RESULT: $res" if($DEBUG);
    $self->{qcount}++;
    return $sth;
}



# ------------------------------------------------------------------------
# SELECT
# ------------------------------------------------------------------------


sub fetch {
    my $self = shift;
    my $query   = $self->{schema}->query(@_);
    my $sth     = $self->execute($query->sql, undef, $query->bind_values);

    if ($query->wantobjects) {
        return $self->objects($query, $sth);
    }
    else {
        return $self->simple_objects($query, $sth);
    }
}


sub simple_objects {
    my $self    = shift;
    my $query   = shift;
    my $sth     = shift;

    my @names = map {$_->_name} $query->acolumns;
    my $class = '_' . join('_', @names);

    {
        no strict 'refs';
        if (!@{$class .'::ISA'}) {
            push(@{$class.'::ISA'}, 'Class::Accessor::Fast');
            $class->mk_accessors(@names);
        };
    }

    my @returns;
    while (my $row = $sth->fetchrow_arrayref) {
        my $hash = {};
        my $i = 0;
        map {$hash->{$_} = $row->[$i++]} @names;
        push(@returns, $class->new($hash));
    }
    die $self->{dbh}->errstr if ($self->{dbh}->errstr);

    warn 'debug: # returns: '. scalar(@returns) if($DEBUG);

    if (wantarray) {
        return @returns;
    }
    elsif (@returns > 1) {
        die "Multiple results returned - single result required";
    }
    return $returns[0];
}


sub objects {
    my $self    = shift;
    my $query   = shift;
    my $sth     = shift;
    my @acols   = $query->acolumns;

    my @returns;

    while (my $row = $sth->fetchrow_arrayref) {
        my %objs;
        my $i = 0;
        my $first;
        my @references;

        foreach my $col (map {$_->_column} @acols) {
            if (!$col or !ref($col)) {
                confess "Missing column ". $col;
            }
            my $class = $col->table->class;
            if (!$class) {
                die "Missing class for table ". $col->table->name;
            }
            my $set   = 'set_' . $col->name;

            if (!$objs{$class}) {
                $objs{$class} = $class->new;
            }
            my $obj = $objs{$class};
            $first ||= $obj;

            $obj->$set($row->[$i++]);

            if ($col->primary and defined($row->[$i-1])) {
                $obj->_in_storage(1);
            }
#    warn "$set ".$row->[$i-1];
            if (my $ref = $col->references) {
                # FIXME need to check that refclass was part of the query.
                # otherwise we are creating objects that were not wanted...
                my $refclass = $ref->table->class;
                push(@references, $col);
            }
        }

        foreach my $r (@references) {
            next unless($objs{$r->table->class}->_in_storage);
            if (my $target = $objs{$r->references->table->class}) {
                my $set = 'set_' . $r->name;
                if ($target->_in_storage) {
                    $objs{$r->table->class}->$set($target);
                }
                else {
                    $objs{$r->table->class}->$set(undef); # FIXME: autoload based on key value
                }
            }
        }

        foreach my $o (values %objs) {
            $o->{_changed} = {};
        }
        push(@returns, $first);
    }
    die $self->{dbh}->errstr if ($self->{dbh}->errstr);

    warn 'debug: # returns: '. scalar(@returns) if($DEBUG);

    if (wantarray) {
        return @returns;
    }
    elsif (@returns > 1) {
        die "Multiple results returned - single result required";
    }
    return $returns[0];
}


sub insert {
    my $self = shift;
    foreach my $obj (@_) {
        UNIVERSAL::isa($obj, 'SQL::DB::Object') ||
            croak "Can only insert SQL::DB::Object: $obj";
        !$obj->_in_storage || carp "Inserting item already in a storage";
        $self->do($obj->q_insert);
        $obj->_in_storage(1);
    }
}


sub update {
    my $self = shift;
    foreach my $obj (@_) {
        UNIVERSAL::isa($obj, 'SQL::DB::Object') ||
            croak "Can only update SQL::DB::Object";
#        $obj->_in_storage || croak "Can only update items already in storage";
        if ($self->do($obj->q_update) != 1) {
            die 'UPDATE for '. ref($obj) . ' object failed';
        }
    }
}


sub delete {
    my $self = shift;
    foreach my $obj (@_) {
        UNIVERSAL::isa($obj, 'SQL::DB::Object') ||
            croak "Can only delete SQL::DB::Object";
        $obj->_in_storage || croak "Can only delete items already in storage";
        if ($self->do($obj->q_delete) != 1) {
            die 'DELETE for '. ref($obj) . ' object failed';
        }
    }
}


sub qcount {
    my $self = shift;
    return $self->{qcount};
}


sub disconnect {
    my $self = shift;
    if ($self->{dbh}) {
        warn 'debug: Disconnecting from DBI' if($DEBUG);
        $self->{dbh}->disconnect;
    }
    return;
}


DESTROY {
    my $self = shift;
    $self->disconnect;
    return;
}


1;
__END__

=head1 NAME

SQL::DB - Perl interface to SQL Databases

=head1 VERSION

0.04. Development release.

=head1 SYNOPSIS

  use SQL::DB;
  my $db = SQL::DB->new();

  $db->define([
      table  => 'persons',
      class  => 'Person',
      column => [name => 'id',      type => 'INTEGER', primary => 1],
      column => [name => 'name',    type => 'VARCHAR(255)'],
      column => [name => 'age',     type => 'INTEGER'],
      column => [name => 'address', type => 'INTEGER',
                                    ref  => 'addresses(id)',
                                    null => 1],
      column => [name => 'parent',  type => 'INTEGER',
                                    ref  => 'persons(id)',
                                    null => 1],
      index  => 'name',
  ]);

  $db->define([
      table        => 'addresses',
      class        => 'Address',
      column       => [name => 'id',   type => 'INTEGER', primary => 1],
      column       => [name => 'kind', type => 'INTEGER'],
      column       => [name => 'city', type => 'INTEGER'],
  ]);

  $db->connect('dbi:SQLite:/tmp/sqldbtest.db', 'user', 'pass', {});
  $db->deploy;

  my $person  = Person::Abstract->new;
  $db->do(
    insert => [$person->id, $person->name, $person->age],
    values => [1, 'Homer', 43],
  );

  my $address  = Address::Abstract->new;
  $db->do(
    insert => [$address->id, $address->kind, $address->city],
    values => [2, 'residential', 'Springfield'],
  );

  $db->do(
    update => $person,
    set    => [$person->address->set(2)],
    where  => $person->name == 'Homer',
  );


  my $p   = Person::Abstract->new;
  my $add = Address::Abstract->new;

  my @items = $db->fetch(
    select    => [$p->name, $p->age, $add->city],
    from      => $p,
    left_join => $add,
    on        => $add->id == $p->address,
    where     => $add->city == 'Springfield' & $p->age > 40,
    order_by  => $p->age->desc,
    limit     => 10,
  );

  foreach my $item (@items) {
      print $item->name, '(',$item->age,') lives in ', $item->city, "\n";
  }
  # "Homer(38) lives in Springfield"

=head1 DESCRIPTION

B<SQL::DB> provides a low-level interface to SQL databases using Perl
objects and logic operators. It is not quite an Object Mapping Layer
(such as L<Class::DBI>) and is also not quite an an abstraction
(like L<SQL::Abstract>). It falls somewhere inbetween.

For a more complete introduction see L<SQL::DB::Tutorial>.

=head1 METHODS

=head2 new(@def)

Create a new SQL::DB object. @def is an optional schema definition
according to L<SQL::DB::Schema>.

=head2 define(@def)

Add to the schema definition. The mandatory @def must be a list of
ARRAY refs as required by L<SQL::DB::Schema>.

=head2 schema($schema)

Returns the current schema object. The optional $schema will set the
current value. Will croak if $schema is not an L<SQL::DB::Schema> object.

=head2 connect($dbi, $user, $pass, $attrs)

Connect to a database. The parameters are passed directly to
L<DBI>->connect.

=head2 connect_cached($dbi, $user, $pass, $attrs)

Connect to a database, potentially reusing an existing connection.
The parameters are passed directly to L<DBI>->connect_cached. Useful
when running under persistent environments.

=head2 dbh

Returns the L<DBI> database handle we are connected with.

=head2 deploy

Runs the CREATE TABLE statements necessary to create the
$schema in the database. Will warn on any tables that already exist.
Will croak if the schema has not yet been defined.

=head2 do(@query)

Constructs an SQL::DB::Query object using @query, and runs that query
against the connected database. Croaks if an error occurs. This is the
method to use for any statement that doesn't retrieve values (eg INSERT,
UPDATE and DELETE). Returns whatever value the underlying L<DBI>->do
call returns.

=head2 fetch(@query)

Constructs an SQL::DB::Query object using @query, and runs that query
against the connected database. Croaks if an error occurs. This 
method can be used for any SELECT-type statement that retrieves rows.

If the query used a simple "select" then returns a list of simple
Class::Accessor-based objects whose method names correspond to the
columns or functions in the query.

If the query used a "selecto" then returns a list of SQL::DB::Object
-based objects.

=head2 insert($sqlobject)

This is a shortcut for $db->do($sqlobject->q_insert). See
L<SQL::DB::Object> for what the q_insert() method does.

=head2 update($sqlobject)

This is a shortcut for $db->do($sqlobject->q_update). See
L<SQL::DB::Object> for what the q_update() method does.

=head2 delete($sqlobject)

This is a shortcut for $db->do($sqlobject->q_delete). See
L<SQL::DB::Object> for what the q_delete() method does.

=head2 qcount

Returns the number of successful queries that have been run.

=head1 DEBUGGING

If $SQL::DB::DEBUG is set to a true value then SQL
queries and other important actions are 'warn'ed to STDERR

=head1 SEE ALSO

L<SQL::Abstract>, L<SQL::Builder>, L<Class::DBI>, L<Tangram>

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
