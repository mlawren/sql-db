package Test::SQL::DB;
use strict;
use warnings;
use File::ShareDir qw/dist_dir/;
use Log::Any qw/$log/;
use Moo::Role;
use Path::Class;
use SQL::DBx::Deploy;

our $VERSION = '0.971.0';

sub _clean_database {
    my $self = shift;

    # The lib ("prove -Ilib t/*") case:
    my $dir1 =
      file(__FILE__)
      ->parent->parent->parent->parent->subdir( 'share', $self->dbd );

    # The blib ("make test") case
    my $dir2 =
      file(__FILE__)
      ->parent->parent->parent->parent->parent->subdir( 'share', $self->dbd );

    if ( -d $dir1 ) {
        $self->run_dir( $dir1->subdir('clean') );
    }
    elsif ( -d $dir2 ) {
        $self->run_dir( $dir2->subdir('clean') );
    }
    else {
        # The "installed" case
        my $distdir = dir( dist_dir('SQL-DB'), $self->dbd, 'clean' );
        $self->run_dir($distdir);
    }
}

Moo::Role->apply_role_to_package( 'SQL::DB', __PACKAGE__ );

1;
