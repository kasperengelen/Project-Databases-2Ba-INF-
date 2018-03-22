--Schema for system tables
CREATE SCHEMA SYSTEM;

--Schema for unit-testing purposes
CREATE SCHEMA TEST;

-- Table for keeping track of users
CREATE TABLE SYSTEM.user_accounts (
    userid        SERIAL,
    fname         VARCHAR(255) NOT NULL,
    lname         VARCHAR(255) NOT NULL,
    email         VARCHAR(255) UNIQUE NOT NULL,
    passwd        VARCHAR(255) NOT NULL,
    register_date TIMESTAMP    DEFAULT now() NOT NULL,
    admin         BOOLEAN      DEFAULT FALSE NOT NULL,

    PRIMARY KEY(userid)
);

-- table that keeps track of datasets
CREATE TABLE SYSTEM.datasets (
    setid         	SERIAL,
    setname       	VARCHAR(255) NOT NULL,
	description		VARCHAR(255) NOT NULL,
    creation_date 	TIMESTAMP    DEFAULT now(),

    PRIMARY KEY(setid)
);

-- table that links users to datasets
CREATE TABLE SYSTEM.set_permissions (
    userid            INTEGER,
    setid             INTEGER,
    permission_type   VARCHAR(255),

    PRIMARY KEY(userid, setid),
    CHECK(permission_type IN ('admin', 'write', 'read')),
    FOREIGN KEY(userid) REFERENCES SYSTEM.user_accounts(userid) ON DELETE CASCADE,
    FOREIGN KEY(setid) REFERENCES SYSTEM.datasets(setid) ON DELETE CASCADE
);

--Table that should be made for every dataset to store all transformations performed on the data
/*CREATE TABLE dataset_history(
	setid INTEGER,
	table_name VARCHAR(255),
	attribute VARCHAR(255),
	transformation_type VARCHAR(255),
	parameter VARCHAR(255),
	origin_table VARCHAR(255),
	transformation_date TIMESTAMP DEFAULT NOW(),

	FOREIGN KEY(setid) REFERENCES datasets(setid) ON DELETE CASCADE);
);*/

--TRIGGER to delete all the data if the admin is deleted (needs to be modified for more than 1 admin)
/*CREATE FUNCTION delete_clean() RETURNS TRIGGER AS $BODY$
BEGIN
    DELETE FROM datasets WHERE datasets.setid IN
    (SELECT set_permissions.setid FROM set_permissions WHERE
    set_permissions.userid = OLD.userid AND set_permissions.permission_type = 'admin');
    RETURN OLD;
END;
$BODY$ LANGUAGE PLPGSQL;
 
CREATE TRIGGER deleted_user BEFORE DELETE ON user_accounts
FOR EACH ROW EXECUTE PROCEDURE delete_clean();*/
