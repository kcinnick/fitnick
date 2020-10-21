-- Table: activity.log

-- DROP TABLE activity.log;

CREATE TABLE activity.log
(
    activity_id integer,
    activity_name character varying COLLATE pg_catalog."default",
    log_id character varying COLLATE pg_catalog."default" NOT NULL,
    calories numeric(10,5),
    distance numeric(10,5),
    duration integer,
    duration_minutes numeric(10,5),
    start_date date,
    start_time character varying COLLATE pg_catalog."default",
    steps integer,
    CONSTRAINT log_pkey PRIMARY KEY (log_id)
)

TABLESPACE pg_default;

ALTER TABLE activity.log
    OWNER to postgres;
