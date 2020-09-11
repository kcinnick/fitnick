-- Table: heart.daily

-- DROP TABLE heart.daily;

CREATE TABLE heart.daily
(
    type character varying COLLATE pg_catalog."default",
    minutes numeric(10,5),
    date date NOT NULL,
    calories numeric(10,5),
    resting_heart_rate integer,
    CONSTRAINT daily_type_date_key UNIQUE (type, date)
)

TABLESPACE pg_default;

ALTER TABLE heart.daily
    OWNER to postgres;
