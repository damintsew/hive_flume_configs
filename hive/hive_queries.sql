CREATE database adamintsev;

CREATE EXTERNAL TABLE sales(
	id BIGINT,
	category  STRING,
	name STRING,
	price BIGINT,
	date TIMESTAMP,
	ipaddr STRING)
    PARTITIONED by (sell_date TIMESTAMP)
    ROW FORMAT DELIMITED
    fields terminated by '\073'
    lines terminated by '\n'
    STORED AS TEXTFILE
    location '/user/adamintsev/events/'


ALTER TABLE sales ADD PARTITION (sell_date='2015-03-01') LOCATION '/user/adamintsev/events/2015/03/01';
ALTER TABLE sales ADD PARTITION (sell_date='2015-03-02') LOCATION '/user/adamintsev/events/2015/03/02';
ALTER TABLE sales ADD PARTITION (sell_date='2015-03-03') LOCATION '/user/adamintsev/events/2015/03/03';
ALTER TABLE sales ADD PARTITION (sell_date='2015-03-04') LOCATION '/user/adamintsev/events/2015/03/04';
ALTER TABLE sales ADD PARTITION (sell_date='2015-03-05') LOCATION '/user/adamintsev/events/2015/03/05';
ALTER TABLE sales ADD PARTITION (sell_date='2015-03-06') LOCATION '/user/adamintsev/events/2015/03/06';
ALTER TABLE sales ADD PARTITION (sell_date='2015-03-07') LOCATION '/user/adamintsev/events/2015/03/07';

CREATE EXTERNAL TABLE countries(
  geoname_id INT,
  locale_code STRING,
  continent_code STRING,
  continent_name STRING,
  country_iso_code STRING,
  country_name STRING
)
    ROW FORMAT DELIMITED
    fields terminated by ','
    lines terminated by '\n'
    STORED AS TEXTFILE
location '/user/adamintsev/geo/country'
tblproperties(
  "skip.header.line.count"="1"
);

CREATE EXTERNAL TABLE ip_geocode(
  network STRING,
  geoname_id INT,
  registered_country_geoname_id INT,
  represented_country_geoname_id INT,
  is_anonymous_proxy BOOLEAN,
  is_satellite_provider BOOLEAN
)
    ROW FORMAT DELIMITED
    fields terminated by ','
    lines terminated by '\n'
    STORED AS TEXTFILE
location '/user/adamintsev/geo/ip'
tblproperties(
  "skip.header.line.count"="1"
);
