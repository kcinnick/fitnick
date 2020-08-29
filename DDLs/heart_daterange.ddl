-- Table: heart.daterange

DROP TABLE heart.daterange;

CREATE TABLE heart.daterange
(
    type character varying COLLATE pg_catalog."default",
	base_date date,
	end_date date,
    minutes numeric(10,5),
    calories numeric(10,5),
	UNIQUE (base_date, end_date, type)
)

TABLESPACE pg_default;

ALTER TABLE heart.daterange
    OWNER to postgres;
