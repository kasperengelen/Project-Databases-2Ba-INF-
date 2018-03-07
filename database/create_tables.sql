-- Table for keeping track of users
CREATE TABLE user_accounts (
    userid        SERIAL,
    fname         VARCHAR(255) NOT NULL,
    lname         VARCHAR(255) NOT NULL,
    email         VARCHAR(255) UNIQUE NOT NULL,
    passwd        VARCHAR(255) NOT NULL,
    register_date TIMESTAMP    DEFAULT now(),

    PRIMARY KEY(userid)
);

-- table that keeps track of datasets
CREATE TABLE datasets (
    setid         	SERIAL,
    setname       	VARCHAR(255) NOT NULL,
	description		VARCHAR(255) NOT NULL,
    creation_data 	TIMESTAMP    DEFAULT now(),

    PRIMARY KEY(setid)
);

-- table that links tables to datasets
CREATE TABLE tables (
    tableid SERIAL UNIQUE,
    setid INTEGER,
    displayname VARCHAR(255) NOT NULL,
    
    PRIMARY KEY(tableid, setid),
    FOREIGN KEY (setid) REFERENCES datasets(setid) ON DELETE CASCADE
);

-- table that links users to datasets
CREATE TABLE set_permissions (
    userid          INTEGER,
    setid           INTEGER,
    permission_type VARCHAR(255),

    PRIMARY KEY(userid, setid),
    CHECK(permission_type IN ('admin', 'write', 'read')),
    FOREIGN KEY(userid) REFERENCES user_accounts(userid) ON DELETE CASCADE
);
