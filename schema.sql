DROP TABLE IF EXISTS trip CASCADE;
DROP TABLE IF EXISTS time_dim CASCADE;
DROP TABLE IF EXISTS zone CASCADE;

CREATE TABLE zone (
    zone_id       integer PRIMARY KEY,
    borough       varchar(50),
    zone_name     varchar(80),
    service_zone  varchar(40),
    is_airport    boolean,
    is_manhattan  boolean
);

CREATE TABLE time_dim (
    date_id         serial PRIMARY KEY,
    pickup_date     date UNIQUE NOT NULL,
    pickup_year     integer NOT NULL,
    pickup_month    integer NOT NULL,
    pickup_day      integer NOT NULL,
    pickup_week     integer NOT NULL,
    pickup_weekday  integer NOT NULL,
    is_weekend      boolean NOT NULL
);

CREATE TABLE trip (
    trip_id           bigserial PRIMARY KEY,
    pickup_datetime   timestamp without time zone NOT NULL,
    dropoff_datetime  timestamp without time zone NOT NULL,
    date_id           integer NOT NULL REFERENCES time_dim(date_id),
    pu_zone_id        integer NOT NULL REFERENCES zone(zone_id),
    do_zone_id        integer NOT NULL REFERENCES zone(zone_id),
    passenger_count   integer,
    trip_distance_mi  numeric(10,2),
    trip_duration_min numeric(10,2),
    fare_amount       numeric(9,2),
    extra             numeric(9,2),
    mta_tax           numeric(9,2),
    tip_amount        numeric(9,2),
    tolls_amount      numeric(9,2),
    total_amount      numeric(9,2),
    payment_type      integer
);

CREATE INDEX idx_trip_date ON trip(date_id);
CREATE INDEX idx_trip_pu_zone ON trip(pu_zone_id);
CREATE INDEX idx_trip_pickup_dt ON trip(pickup_datetime);
