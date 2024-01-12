-- This file should contain all code required to create & seed database tables.
DROP TABLE IF EXISTS Reading;
DROP TABLE IF EXISTS Ride;
DROP TABLE IF EXISTS Bike;
DROP TABLE IF EXISTS Rider;
DROP TABLE IF EXISTS Address;

CREATE TABLE Address(
    address_id SERIAL PRIMARY KEY,
    first_line TEXT NOT NULL,
    second_line TEXT,
    city TEXT NOT NULL,
    postcode TEXT NOT NULL,
    UNIQUE (first_line,city,postcode)
);

CREATE TABLE Rider(
    rider_id INT PRIMARY KEY,
    address_id INT NOT NULL,
    FOREIGN KEY (address_id)
        REFERENCES Address(address_id)
        ON DELETE CASCADE,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    birthdate DATE NOT NULL,
    height SMALLINT NOT NULL,
    weight SMALLINT NOT NULL,
    email TEXT NOT NULL,
    gender TEXT,
    account_created DATE NOT NULL
);

CREATE TABLE Bike(
    bike_id SERIAL PRIMARY KEY,
    serial_number TEXT NOT NULL,
    UNIQUE (serial_number)
);

CREATE TABLE Ride(
    ride_id SERIAL PRIMARY KEY,
    rider_id INT NOT NULL,
    FOREIGN KEY (rider_id)
        REFERENCES Rider(rider_id)
        ON DELETE CASCADE,
    bike_id INT NOT NULL,
    FOREIGN KEY (bike_id)
        REFERENCES Bike(bike_id)
        ON DELETE CASCADE,
    start_time TIMESTAMP,
    UNIQUE (rider_id,bike_id,start_time)
);

CREATE INDEX idx_ride_time ON ride(start_time DESC);


CREATE TABLE Reading(
    reading_id SERIAL PRIMARY KEY,
    ride_id INT NOT NULL,
    FOREIGN KEY (ride_id)
        REFERENCES Ride(ride_id)
        ON DELETE CASCADE,
    heart_rate SMALLINT NOT NULL,
    power FLOAT NOT NULL,
    rpm SMALLINT NOT NULL, 
    resistance SMALLINT NOT NULL,
    elapsed_time INT NOT NULL,
    UNIQUE (ride_id,elapsed_time)
);

CREATE INDEX idx_ride_id ON reading(ride_id);