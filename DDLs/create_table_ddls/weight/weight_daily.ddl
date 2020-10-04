-- Table: weight.daily

-- DROP TABLE weight.daily;

CREATE TABLE weight.daily
(
    date date NOT NULL,
    pounds numeric(4,1)
)

TABLESPACE pg_default;

ALTER TABLE weight.daily
    OWNER to postgres;

GRANT ALL ON TABLE weight.daily TO postgres;
