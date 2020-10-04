CREATE TABLE sleep.summary
(
    date date,
    deep character varying,
    light character varying,
    rem character varying,
    wake character varying,
    total_minutes_asleep character varying,
    total_time_in_bed character varying
);

ALTER TABLE sleep.summary
    OWNER to postgres;
