-- Table: heart.daily

-- DROP TABLE heart.daily;

CREATE TABLE heart.daily
(
    type character varying COLLATE pg_catalog."default",
    minutes numeric(10,5),
    date date NOT NULL,
    calories numeric(10,5),
	UNIQUE (type, date)
)

TABLESPACE pg_default;

ALTER TABLE heart.daily
    OWNER to postgres;
