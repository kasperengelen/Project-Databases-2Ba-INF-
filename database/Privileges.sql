GRANT CREATE ON DATABASE projectdb18 TO dbadmin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA system TO dbadmin;
GRANT ALL PRIVILEGES ON SCHEMA system TO dbadmin;
GRANT USAGE, SELECT ON SEQUENCE system.datasets_setid_seq TO dbadmin;
GRANT USAGE, SELECT ON SEQUENCE system.user_accounts_userid_seq TO dbadmin;
