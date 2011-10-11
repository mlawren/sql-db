package test::Deploy::Pg;

1;

# Only ADD lines to this file. Never delete them!

__DATA__
- sql: |
   CREATE TABLE actors (
       id integer primary key,
       name varchar
   )

- sql: |
   CREATE TABLE films (
       id integer primary key,
       title varchar
   )

- perl: |
   print "Perl sections are eval'd\n";
