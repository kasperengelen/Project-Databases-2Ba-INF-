CREATE SCHEMA SYSTEM;

-- Table for keeping track of users.
CREATE TABLE SYSTEM.user_accounts (
    userid        SERIAL,
    fname         VARCHAR(255) NOT NULL,
    lname         VARCHAR(255) NOT NULL,
    email         VARCHAR(255) UNIQUE NOT NULL,
    passwd        VARCHAR(255) NOT NULL,
    register_date TIMESTAMP    DEFAULT now() NOT NULL,
    admin         BOOLEAN      DEFAULT FALSE NOT NULL,
    active        BOOLEAN      DEFAULT TRUE  NOT NULL,

    PRIMARY KEY(userid)
);

-- Table that keeps track of datasets.
CREATE TABLE SYSTEM.datasets (
    setid         	SERIAL,
    setname       	VARCHAR(255) NOT NULL,
	description		VARCHAR(255) NOT NULL,
    creation_date 	TIMESTAMP    DEFAULT now(),

    PRIMARY KEY(setid)
);

-- Table that links users to datasets with the correct permissions.
CREATE TABLE SYSTEM.set_permissions (
    userid            INTEGER,
    setid             INTEGER,
    permission_type   VARCHAR(255),

    PRIMARY KEY(userid, setid),
    CHECK(permission_type IN ('admin', 'write', 'read')),
    FOREIGN KEY(userid) REFERENCES SYSTEM.user_accounts(userid) ON DELETE CASCADE,
    FOREIGN KEY(setid) REFERENCES SYSTEM.datasets(setid) ON DELETE CASCADE
);

-- Table containing history of the transformations perfomed in the system.
CREATE TABLE SYSTEM.DATASET_HISTORY(
	setid INTEGER,
	table_name VARCHAR(255) NOT NULL,
	attribute VARCHAR(255) NOT NULL,
	transformation_type INTEGER NOT NULL,
	parameters VARCHAR(255) ARRAY NOT NULL,
	origin_table VARCHAR(255) NOT NULL,
	transformation_id SERIAL UNIQUE,
	transformation_date TIMESTAMP DEFAULT NOW(),

	FOREIGN KEY(setid) REFERENCES SYSTEM.datasets(setid) ON DELETE CASCADE
);

-- Trigger to delete all the corresponding data if the last admin of the data is deleted.
CREATE FUNCTION delete_clean() RETURNS TRIGGER AS $BODY$
BEGIN
    DELETE FROM system.datasets WHERE system.datasets.setid IN (SELECT setid FROM system.set_permissions 
	WHERE system.set_permissions.userid != OLD.userid AND system.set_permissions.permission_type = 'admin');
	/*AND system.datasets.setid NOT IN (SELECT count(*), setid, set_permissions FROM system.set_permissions GROUP BY setid, 
	set_permissions HAVING count(*) > 1);*/
    RETURN OLD;
END;
$BODY$ LANGUAGE PLPGSQL;
 
CREATE TRIGGER deleted_user BEFORE DELETE ON SYSTEM.user_accounts
FOR EACH ROW EXECUTE PROCEDURE delete_clean();-
