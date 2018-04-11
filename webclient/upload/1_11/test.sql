CREATE TABLE test (
    userid	  INT
    email        VARCHAR(255) NOT NULL,
    register_date TIMESTAMP    DEFAULT now() NOT NULL,

    PRIMARY KEY(userid)
);

INSERT INTO test VALUES(1, 'test', 01/01/2018)
INSERT INTO test VALUES(2, 'Luuk', 15/01/2018)
INSERT INTO test VALUES(3, 'Hoi', 20/01/2018)
