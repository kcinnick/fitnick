CREATE TABLE sleep.level
(
    date date NOT NULL,
    type_ character varying,
    count_ character varying,
    minutes character varying,
    thirty_day_avg_minutes character varying,
    CONSTRAINT date_type_key UNIQUE (date, type_)
);

ALTER TABLE sleep.level
    OWNER to postgres;
