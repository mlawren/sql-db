CREATE OR REPLACE FUNCTION create_deploy_table() RETURNS VOID AS $$
BEGIN
    CREATE TABLE _deploy (
        app VARCHAR(40) NOT NULL PRIMARY KEY,
        seq INTEGER NOT NULL DEFAULT 0,
        ctime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        type VARCHAR(20),
        data VARCHAR
    );

EXCEPTION WHEN duplicate_table THEN
    -- Do nothing
END;
$$ LANGUAGE plpgsql;

SELECT create_deploy_table();

DROP FUNCTION create_deploy_table();

CREATE OR REPLACE FUNCTION deploy_autoinc() RETURNS trigger AS $$
BEGIN
    RAISE NOTICE 'seq=%s', NEW.seq;
    NEW.seq := NEW.seq + 1;
    RAISE NOTICE 'seq=%s', NEW.seq;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS au__deploy ON _deploy;

CREATE TRIGGER au__deploy BEFORE UPDATE ON _deploy
FOR EACH ROW EXECUTE PROCEDURE deploy_autoinc();

