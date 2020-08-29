-- Database: fitbit

-- DROP DATABASE fitbit;

CREATE DATABASE fitbit
    WITH
    OWNER = kcinnick
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF8'
    LC_CTYPE = 'en_US.UTF8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

CREATE DATABASE fitbit_test
    WITH
    OWNER = kcinnick
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF8'
    LC_CTYPE = 'en_US.UTF8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;
