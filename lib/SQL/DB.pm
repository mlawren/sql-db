package SQL::DB;
use 5.006;
use strict;
use warnings;
use base qw(SQL::DB::Schema);
use Carp qw(carp croak confess);
use DBI;
use UNIVERSAL qw(isa);
use SQL::DB::Schema;
use SQL::DB::Function;
use Class::Accessor::Fast;

use Data::Dumper;
$Data::Dumper::Indent = 1;

our $VERSION = '0.05';
our $DEBUG   = 0;
our @EXPORT_OK = @SQL::DB::Function::EXPORT_OK;

foreach (@EXPORT_OK) {
    no strict 'refs';
    *{$_} = *{'SQL::DB::Function::'.$_};
}


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = bless($class->SUPER::new(@_), $class);
    $self->define([
        table => 'sqldb',
        class => 'SQL::DB::Sequence',
        column => [name => 'name', type => 'VARCHAR(32)', unique => 1],
        column => [name => 'val', type => 'INTEGER'],
    ]);
    return $self;
}


sub connect {
    my $self = shift;

    my ($dbi,$user,$pass,$attrs) = @_;

    if (my $dbh = DBI->connect($dbi,$user,$pass,$attrs)) {
        $self->{sqldb_dbh} = $dbh;
    }
    else {
        croak $DBI::errstr;
    }

    $self->{sqldb_dbi}    = $dbi;
    $self->{sqldb_user}   = $user;
    $self->{sqldb_pass}   = $pass;
    $self->{sqldb_attrs}  = $attrs;
    $self->{sqldb_qcount} = 0;

    warn "debug: connected to $dbi" if($DEBUG);
    return;
}


sub connect_cached {
    my $self = shift;

    my ($dbi,$user,$pass,$attrs) = @_;

    if (my $dbh = DBI->connect_cached($dbi,$user,$pass,$attrs)) {
        $self->{sqldb_dbh} = $dbh;
    }
    else {
        croak $DBI::errstr;
    }

    $self->{sqldb_dbi}    = $dbi;
    $self->{sqldb_user}   = $user;
    $self->{sqldb_pass}   = $pass;
    $self->{sqldb_attrs}  = $attrs;
    $self->{sqldb_qcount} = 0;

    warn "debug: connect_cached to $dbi" if($DEBUG);
    return;
}


sub dbh {
    my $self = shift;
    return $self->{sqldb_dbh};
}


sub deploy {
    my $self = shift;

    foreach my $table ($self->tables) {
        my $sth = $self->{sqldb_dbh}->table_info('', '', $table->name, 'TABLE');
        if (!$sth) {
            die $DBI::errstr;
        }

        if ($sth->fetch) {
            carp 'Table '. $table->name .' already exists - not deploying';
            next;
        }

        foreach my $action ($table->sql_create) {
            warn "debug: $action" if($DEBUG);
            if (!$self->{sqldb_dbh}->do($action)) {
                die $self->{sqldb_dbh}->errstr;
            }
        }
    }
}


sub create_seq {
    my $self = shift;
    my $name = shift || croak 'usage: $db->create_seq($name)';

    $self->{sqldb_dbi} || croak 'Must be connected before calling create_seq';

    my $s = SQL::DB::Sequence::Abstract->new;

    if (eval {
        $self->do(
            insert  => [$s->name, $s->val],
            values  => [$name, 0],
        );
        }) {
        return 1;
    }
    warn "CreateSequence: $@";
    return;
}


sub seq {
    my $self = shift;
    my $name = shift || croak 'usage: $db->seq($name)';

    $self->{sqldb_dbi} || croak 'Must be connected before calling seq';

    $self->{sqldb_dbh}->begin_work;
    my $s = SQL::DB::Sequence::Abstract->new;
    my $r;

    if (eval {
        $r = $self->fetch1(
            select     => [$s->val],
            from       => $s,
            for_update => ($self->{sqldb_dbi} !~ m/sqlite/i),
            where      => $s->name == $name,
        );

        die "Can't find sequence $name" unless($r);

        $self->do(
            update  => [$s->val->set($r->val + 1)],
            where   => $s->name == $name,
        );

        1;}) {

        $self->{sqldb_dbh}->commit;
        return $r->val + 1;
    }
    else {
        my $tmp = $@;
        eval {$self->{sqldb_dbh}->rollback;};
        croak "seq: $tmp";
    }
    return;
}


# ------------------------------------------------------------------------
# For everything other than SELECT
# ------------------------------------------------------------------------

sub do {
    my $self        = shift;
    my $query       = $self->query(@_);

    my $rv;
    eval {
        $rv = $self->{sqldb_dbh}->do("$query", undef, $query->bind_values);
    };

    if ($@ or !defined($rv)) {
        croak "DBI::do $DBI::errstr $@: Query was:\n". $query->_as_string;
    }

    $self->{sqldb_qcount}++;

    carp 'debug: '. $query->_as_string if($DEBUG);
    return $rv;
}


sub execute {
    my $self = shift;
    my ($sql,$attrs,@bind) = @_;

    my $sth;
    eval {
        $sth = $self->{sqldb_dbh}->prepare($sql);
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
    $self->{sqldb_qcount}++;
    return $sth;
}



# ------------------------------------------------------------------------
# SELECT
# ------------------------------------------------------------------------


sub fetch {
    my $self = shift;
    my $query   = $self->query(@_);
    my $sth     = $self->execute("$query", undef, $query->bind_values);

    if ($query->wantobjects) {
        return $self->objects($query, $sth);
    }
    else {
        return $self->simple_objects($query, $sth);
    }
}


sub fetch1 {
    my $self = shift;
    my $query   = $self->query(@_);
    my $sth     = $self->execute($query, undef, $query->bind_values);

    my @results;
    if ($query->wantobjects) {
        @results = $self->objects($query, $sth);
    }
    else {
        @results = $self->simple_objects($query, $sth);
    }
    return $results[0];
}


sub simple_objects {
    my $self    = shift;
    my $query   = shift;
    my $sth     = shift;

    my @names = map {$_->_name} $query->acolumns;
    for my $i (0..$#names) {
        $names[$i] =~ s/t\d+\.//;
    }
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
    die $self->{sqldb_dbh}->errstr if ($self->{sqldb_dbh}->errstr);

    warn 'debug: # returns: '. scalar(@returns) if($DEBUG);

    return @returns;
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
    die $self->{sqldb_dbh}->errstr if ($self->{sqldb_dbh}->errstr);

    warn 'debug: # returns: '. scalar(@returns) if($DEBUG);

    return @returns;
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
    return $self->{sqldb_qcount};
}


sub disconnect {
    my $self = shift;
    if ($self->{sqldb_dbh}) {
        warn 'debug: Disconnecting from DBI' if($DEBUG);
        $self->{sqldb_dbh}->disconnect;
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

0.05. Development release.

=head1 SYNOPSIS

  use SQL::DB qw(max min coalesce count);
  my $db = SQL::DB->new(
    [
      table  => 'addresses',
      class  => 'Address',
      column => [name => 'id',   type => 'INTEGER', primary => 1],
      column => [name => 'kind', type => 'INTEGER'],
      column => [name => 'city', type => 'INTEGER'],
    ],
    [
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
    ],
  );

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
    update => [$person->address->set(2)],
    where  => $person->name == 'Homer',
  );


  my $p   = Person::Abstract->new;
  my $add = Address::Abstract->new;

  my $ans = $db->fetch1(
    select    => [count($p->name)->as('count_name'),
                  max($p->age)->as('max_age')],
    from      => $p,
    where     => $p->age > 40,
  );

  print 'Head count: '. $ans->count_name .' Max age:'.$ans->max_age."\n";
  # "Head count: 1 Max age:43"


  my @items = $db->fetch(
    select    => [$p->name, $p->age, $add->city],
    from      => $p,
    left_join => $add,
    on        => $add->id == $p->address,
    where     => ($add->city == 'Springfield') & ($p->age > 40),
    order_by  => $p->age->desc,
    limit     => 10,
  );

  foreach my $item (@items) {
      print $item->name, '(',$item->age,') lives in ', $item->city, "\n";
  }
  # "Homer(43) lives in Springfield"
  return @items # this line for automatic test

=head1 DESCRIPTION

B<SQL::DB> provides a low-level interface to SQL databases using Perl
objects and logic operators. It is not quite an Object Mapping Layer
(such as L<Class::DBI>) and is also not quite an an abstraction
(like L<SQL::Abstract>). It falls somewhere inbetween.

For a more complete introduction see L<SQL::DB::Intro>.

=head1 METHODS

=head2 new(@def)

Create a new SQL::DB object. @def is an optional schema definition
according to L<SQL::DB::Schema>.

=head2 define(@def)

Add to the schema definition. Inherited from L<SQL::DB::Schema>.

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

Runs the CREATE TABLE and CREATE INDEX statements necessary to
create the schema in the database. Will warn on any tables that
already exist.

=head2 query(@query)

Returns an L<SQL::DB::Query> built from @query. This method is useful
when you are creating nested queries and UNION statements.

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

=head2 fetch1(@query)

Is the same as fetch(), but only returns the first element from the
result set. Either you know you will only get one result, or you
should be using some kind of LIMIT statement so that extra rows
are not retrieved by the database.

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
