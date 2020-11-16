CREATE TABLE sleep.summary
(
    date date NOT NULL,
    duration character varying,
    efficiency character varying,
    end_time character varying,
    minutes_asleep character varying,
    minutes_awake character varying,
    start_time character varying,
    time_in_bed character varying,
    log_id character varying,
    CONSTRAINT date_key UNIQUE (date)
);

ALTER TABLE sleep.summary
    OWNER to postgres;
