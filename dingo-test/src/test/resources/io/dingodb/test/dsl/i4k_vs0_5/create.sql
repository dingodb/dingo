--- Currently not used.
CREATE TABLE locations (
    location_id int NOT NULL,
    street_address varchar(40) DEFAULT NULL,
    postal_code varchar(12) DEFAULT NULL,
    city varchar(30) DEFAULT NULL,
    state_province varchar(25) DEFAULT NULL,
    country_id varchar(2) DEFAULT NULL,
    PRIMARY KEY (location_id)
)
