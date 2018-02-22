CREATE DATABASE 

CREATE TABLE user_accounts(
    userid        SERIAL,
    fname         VARCHAR(255) NOT NULL,
    lname         VARCHAR(255) NOT NULL,
    email         VARCHAR(255) UNIQUE NOT NULL,
    passwd        VARCHAR(255) NOT NULL,
    register_date TIMESTAMP    DEFAULT now(),
    PRIMARY KEY(userid)
);
