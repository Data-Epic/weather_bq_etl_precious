-- Create or replace cities table
CREATE OR REPLACE TABLE `weather-pipeline-12.weather_data.cities` (
    id INT64 NOT NULL PRIMARY KEY NOT ENFORCED,  -- Primary key
    city STRING NOT NULL,                         -- City name
    lon FLOAT64,                                 -- Longitude
    lat FLOAT64,                                 -- Latitude
    country STRING,                              -- Country
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Created at timestamp
);

-- Create or replace timezone table
CREATE OR REPLACE TABLE `weather-pipeline-12.weather_data.timezone` (
    timezone INT64 NOT NULL,
    timezone_utc STRING NOT NULL PRIMARY KEY NOT ENFORCED                      -- Primary key
);

-- Create or replace city_tz table
CREATE OR REPLACE TABLE `weather-pipeline-12.weather_data.city_tz` (
    city_id INT64 NOT NULL,                          -- City ID
    tz_id STRING NOT NULL,                           -- Timezone ID
    PRIMARY KEY(city_id, tz_id) NOT ENFORCED,  -- composite primary key
    FOREIGN KEY (city_id) REFERENCES `weather-pipeline-12.weather_data.cities`(id) NOT ENFORCED,
    FOREIGN KEY (tz_id) REFERENCES `weather-pipeline-12.weather_data.timezone`(timezone_utc) NOT ENFORCED
);

-- Create or replace weather table
CREATE OR REPLACE TABLE `weather-pipeline-12.weather_data.weather_condition` (
    id INT64 NOT NULL PRIMARY KEY NOT ENFORCED,      -- Primary key
    main STRING,                                     -- Main weather
    description STRING,                              -- Weather description
    icon STRING                                      -- Weather icon
);

-- Create or replace temperature table
CREATE OR REPLACE TABLE `weather-pipeline-12.weather_data.temperature` (
    id STRING NOT NULL PRIMARY KEY NOT ENFORCED,     -- Primary key
    temp FLOAT64,                                   -- Temperature
    feels_like FLOAT64,                             -- Feels like temperature
    temp_min FLOAT64,                               -- Minimum temperature
    temp_max FLOAT64                                 -- Maximum temperature
);

-- Create or replace pressure table
CREATE OR REPLACE TABLE `weather-pipeline-12.weather_data.pressure` (
    id STRING NOT NULL PRIMARY KEY NOT ENFORCED,     -- Primary key
    pressure INT64,                                  -- Pressure
    sea_level INT64,                                -- Sea level
    grnd_level INT64                                 -- Ground level
);

-- Create or replace wind table
CREATE OR REPLACE TABLE `weather-pipeline-12.weather_data.wind` (
    id STRING NOT NULL PRIMARY KEY NOT ENFORCED,     -- Primary key
    speed FLOAT64,                                   -- Wind speed
    deg INT64                                        -- Wind degree
);

-- Create or replace sun table
CREATE OR REPLACE TABLE `weather-pipeline-12.weather_data.sun` (
    id STRING NOT NULL PRIMARY KEY NOT ENFORCED,     -- Primary key
    sunrise INT64,                                   -- Sunrise time
    sunset INT64                                      -- Sunset time
);

-- Create or replace weather_record table
CREATE OR REPLACE TABLE `weather-pipeline-12.weather_data.weather_record` (
    id STRING NOT NULL PRIMARY KEY NOT ENFORCED,     -- Primary key
    city_id INT64 NOT NULL,                          -- City ID
    weather_condition_id INT64,                      -- Weather condition ID
    temp_id STRING,                                  -- Temperature ID
    pressure_id STRING,                              -- Pressure ID
    wind_id STRING,                                  -- Wind ID
    sun_id STRING,                                   -- Sun ID
    humidity INT64,                                  -- Humidity
    visibility INT64,                                -- Visibility
    cloud INT64,                                     -- Cloud cover
    dt INT64,                                       -- Date/Time
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Recorded at timestamp

    FOREIGN KEY (city_id) REFERENCES `weather-pipeline-12.weather_data.cities`(id) NOT ENFORCED,  -- cities table foreign key
    FOREIGN KEY (weather_condition_id) REFERENCES `weather-pipeline-12.weather_data.weather_condition`(id) NOT ENFORCED, -- weather table foreign key
    FOREIGN KEY (temp_id) REFERENCES `weather-pipeline-12.weather_data.temperature`(id) NOT ENFORCED, -- temperature table foreign key
    FOREIGN KEY (pressure_id) REFERENCES `weather-pipeline-12.weather_data.pressure`(id) NOT ENFORCED,  -- pressure table foreign key
    FOREIGN KEY (wind_id) REFERENCES `weather-pipeline-12.weather_data.wind`(id) NOT ENFORCED,  -- wind table foreign key
    FOREIGN KEY (sun_id) REFERENCES `weather-pipeline-12.weather_data.sun`(id) NOT ENFORCED,  -- sun table foreign key
);
