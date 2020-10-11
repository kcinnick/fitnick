-- Table: bodyfat.daily

-- DROP TABLE bodyfat.daily;

CREATE TABLE bodyfat.daily
(
    date date NOT NULL PRIMARY KEY,
    fat smallint,
    logId VARCHAR(50),
    source VARCHAR(50),
    "time" VARCHAR(50)
)

TABLESPACE pg_default;

ALTER TABLE bodyfat.daily
    OWNER to postgres;

GRANT ALL ON TABLE bodyfat.daily TO postgres;
