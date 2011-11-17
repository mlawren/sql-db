package SQL::DB::Schema;
use strict;
use warnings;
use Moo;
use Log::Any qw/$log/;
use Carp qw/confess/;
use SQL::DB::Expr qw/_quote/;
use Sub::Install qw/install_sub/;
use Sub::Exporter -setup => {
    exports => ['load_schema'],
    groups  => {
        all     => ['load_schema'],
        default => [],
    },
};

our $VERSION = '0.19_11';
my %schema;

# Ordinals for DBI->column_info() results
use constant {
    TABLE_CAT         => 0,
    TABLE_SCHEM       => 1,
    TABLE_NAME        => 2,
    COLUMN_NAME       => 3,
    DATA_TYPE         => 4,
    TYPE_NAME         => 5,
    COLUMN_SIZE       => 6,
    BUFFER_LENGTH     => 7,
    DECIMAL_DIGITS    => 8,
    NUM_PREC_RADIX    => 9,
    NULLABLE          => 10,
    REMARKS           => 11,
    COLUMN_DEF        => 12,
    SQL_DATA_TYPE     => 13,
    SQL_DATETIME_SUB  => 14,
    CHAR_OCTET_LENGTH => 15,
    ORDINAL_POSITION  => 16,
    IS_NULLABLE       => 17,
};

# Object definition

has 'name' => (
    is       => 'rw',
    required => 1,
);

has '_package_root' => (
    is       => 'rw',
    init_arg => undef,
);

has '_tables' => (
    is       => 'ro',
    init_arg => undef,
    default  => sub { {} },
);

sub _getglob { no strict 'refs'; \*{ $_[0] } }

sub BUILD {
    my $self = shift;
    ( my $clean = $self->name ) =~ tr/a-zA-Z0-9/_/cs;
    $self->_package_root( __PACKAGE__ . '::' . $clean );
    $schema{ $self->name } = $self;
    $log->debug( "Schema " . $self->name . " created" );
}

sub define {
    my $self = shift;
    my $data = shift;

    my $package_root = $self->_package_root;
    my $tables       = $self->_tables;

    foreach my $colref (@$data) {
        my $table = $colref->[TABLE_NAME];
        my $srow  = $package_root . '::Srow::' . $table;
        my $urow  = $package_root . '::Urow::' . $table;

        if ( !exists $tables->{$table} ) {

            eval "package $srow; use Moo; extends 'SQL::DB::Expr'";
            eval "package $urow; use Moo; extends 'SQL::DB::Expr'";

  #            @{ *{ _getglob( $srow . '::ISA' ) }{ARRAY} } = ('SQL::DB::Expr');
  #            @{ *{ _getglob( $urow . '::ISA' ) }{ARRAY} } = ('SQL::DB::Expr');

            install_sub(
                {
                    code => sub {
                        my $table_expr = shift;
                        return $table_expr . '.*';
                    },
                    into => $urow,
                    as   => '_columns',
                }
            );
            install_sub(
                {
                    code => sub {
                        my $table_expr = shift;
                        return SQL::DB::Expr->new(
                            _txt => [ $table_expr->_alias . '.*' ] );
                    },
                    into => $srow,
                    as   => '_columns',
                }
            );
        }
        $tables->{$table}++;

        my $col = $colref->[COLUMN_NAME];

        if ( $col eq 'new' ) {
            confess "Column name 'new' (table/view '$table') clashes with "
              . __PACKAGE__ . '!!!';
        }

        use bytes;
        my $type = lc $colref->[TYPE_NAME];

        install_sub(
            {
                code => sub {
                    my $table_expr = shift;
                    SQL::DB::Expr->new(
                        _txt   => [ $table_expr->_alias . '.' . $col ],
                        _btype => $type,
                    );
                },
                into => $srow,
                as   => $col,
            }
        );

        install_sub(
            {
                code => sub {
                    my $table_expr = shift;

                    if (@_) {
                        my $val = shift;
                        return SQL::DB::Expr->new(
                            _txt     => [ $col . ' = ', _quote( $val, $type ) ],
                            _btype   => $type,
                            _bvalues => [$val],
                        );
                    }

                    return SQL::DB::Expr->new(
                        _txt   => [$col],
                        _btype => $type,
                    );
                },
                into => $urow,
                as   => $col,
            }
        );
    }

    return;
}

sub not_known {
    my $self   = shift;
    my $tables = $self->_tables;
    return grep { !exists $tables->{$_} } @_;
}

sub irow {
    my $self = shift;

    my @ret;
    foreach my $name (@_) {
        if ( !exists $self->_tables->{$name} ) {
            confess "Table not defined in schema: $name";
        }
        push( @ret, sub { $name . '(' . join( ',', @_ ) . ')' } );
        return $ret[0] unless (wantarray);
    }
    return @ret;
}

sub srow {
    my $self = shift;

    my @ret;
    foreach my $name (@_) {
        if ( !exists $self->_tables->{$name} ) {
            confess "Table not defined in schema: $name";
        }
        my $class = $self->_package_root . '::Srow::' . $name;
        my $srow = $class->new( _txt => [$name], _alias => $name );
        return $srow unless (wantarray);
        push( @ret, $srow );
    }
    return @ret;
}

sub urow {
    my $self = shift;

    my @ret;
    foreach my $name (@_) {
        if ( !exists $self->_tables->{$name} ) {
            confess "Table not defined in schema: $name";
        }
        my $class = $self->_package_root . '::Urow::' . $name;
        my $urow = $class->new( _txt => [$name] );
        return $urow unless (wantarray);
        push( @ret, $urow );
    }
    return @ret;
}

# Class functions

sub load_schema {
    my $name = shift;
    eval "require $name;";
    if ($@) {
        confess $@;
    }
    elsif ( !exists $schema{$name} ) {
        confess "$name did not load properly";
    }
    $log->debug("load_schema($name) succeeded");
    return $schema{$name};
}

1;

# vim: set tabstop=4 expandtab:
