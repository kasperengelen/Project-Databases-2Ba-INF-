CREATE TABLE user_accounts(
    userid        SERIAL,
    fname         VARCHAR(255) NOT NULL,
    lname         VARCHAR(255) NOT NULL,
    email         VARCHAR(255) UNIQUE NOT NULL,
    passwd        VARCHAR(255) NOT NULL,
    register_date TIMESTAMP    DEFAULT now(),

    PRIMARY KEY(userid)
);

CREATE TABLE datasets (
    setid         SERIAL,
    setname       VARCHAR(255) NOT NULL,
    creation_data TIMESTAMP    DEFAULT now(),

    PRIMARY KEY(setid)
);

CREATE TABLE set_permissions(
    userid          INTEGER,
    setid           INTEGER,
    permission_type VARCHAR(255),

    PRIMARY KEY(userid, setid),
    CHECK(permission_type IN ('admin', 'write', 'read'))
);
