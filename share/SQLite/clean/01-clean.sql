PRAGMA writable_schema = 1;

delete from sqlite_master where type = 'trigger';
delete from sqlite_master where type = 'index';
delete from sqlite_master where type = 'table';

PRAGMA writable_schema = 0;

VACUUM;

PRAGMA INTEGRITY_CHECK;
