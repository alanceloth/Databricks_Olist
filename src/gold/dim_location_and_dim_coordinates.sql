DROP TABLE IF EXISTS olist_dataset.gold.dim_location;
DROP TABLE IF EXISTS olist_dataset.gold.dim_coordinates;

CREATE TABLE olist_dataset.gold.dim_location (
    location_id STRING PRIMARY KEY,
    geolocation_zip_code_prefix STRING,
    geolocation_city STRING,
    geolocation_state STRING
);
CREATE TABLE olist_dataset.gold.dim_coordinates (
    location_id STRING PRIMARY KEY,
    geolocation_lat STRING,
    geolocation_lng STRING
);

INSERT INTO olist_dataset.gold.dim_location (location_id, geolocation_zip_code_prefix, geolocation_city, geolocation_state)
SELECT 
    md5(concat(geolocation_zip_code_prefix, '_', geolocation_city, '_', geolocation_state)) AS location_id,
    geolocation_zip_code_prefix,
    geolocation_city,
    geolocation_state
FROM 
    olist_dataset.silver.silver_olist_geolocation
GROUP BY 
    geolocation_zip_code_prefix, geolocation_city, geolocation_state;

INSERT INTO olist_dataset.gold.dim_coordinates (location_id, geolocation_lat, geolocation_lng)
SELECT 
    md5(concat(geolocation_zip_code_prefix, '_', geolocation_city, '_', geolocation_state)) AS location_id,
    CAST(geolocation_lat AS VARCHAR(55)) AS geolocation_lat,
    CAST(geolocation_lng AS VARCHAR(55)) AS geolocation_lng
FROM 
    olist_dataset.silver.silver_olist_geolocation;

