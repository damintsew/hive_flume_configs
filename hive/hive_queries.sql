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


#Select top 10  most frequently purchased categories

select category, count(category) as counted from sales group by category order by counted DESC LIMIT 10;

create table top_purchased_categories
    ROW FORMAT DELIMITED
    fields terminated by ','
    lines terminated by '\n'
    STORED AS TEXTFILE AS
    select category, count(category) as counted from sales group by category order by counted DESC LIMIT 10



#Select top 10 most frequently purchased product in each category

select category, name from (
    select category, name, counted, row_number() over (partition by category order by counted desc) as rm
        from (select category, name, count(1) counted from sales group by category, name) grouped) row_number
    where rm <= 10 order by category;

create table top_products_for_categories
    ROW FORMAT DELIMITED
    fields terminated by ','
    lines terminated by '\n'
    STORED AS TEXTFILE AS
    select category, name from (
        select category, name, counted, row_number() over (partition by category order by counted desc) as rm
            from (select category, name, count(1) counted from sales group by category, name) grouped) row_number
        where rm <= 10 order by category;



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



#Top sales 10 by countries

set mapreduce.map.memory.mb=1500;
set mapreduce.map.java.opts=-Xmx1000m;
set mapreduce.reduce.memory.mb=1500;
set mapreduce.reduce.java.opts=-Xmx1500m;
SET hive.auto.convert.join=false;

add file hdfs:///user/adamintsev/geo/ip/ipv4_dataset.csv;
ADD jar /home/cloudera/DataGeneration/BigDataUniversity-assembly-2.0.0.jar;
CREATE TEMPORARY FUNCTION extractCountryCode AS 'udf.FindCountryCodeUdf';

#STREAMTABLE version
select c.country_name, grouped.total from
   (select sum(s.price) as total, s.countrycode as cc from
       (select /*+ STREAMTABLE(sales) */ price, extractCountryCode(ipaddr) as countrycode from sales) s group by countrycode) grouped
           left outer join countries c on (c.geoname_id = grouped.cc) order by grouped.total desc limit 10;

select c.country_name, grouped.total from
   (select sum(s.price) as total, s.countrycode as cc from
       (select price, extractCountryCode(ipaddr) as countrycode from sales) s group by countrycode) grouped
           left outer join countries c on (c.geoname_id = grouped.cc) order by grouped.total desc limit 10;



create table top_sales_by_country
    ROW FORMAT DELIMITED
    fields terminated by ','
    lines terminated by '\n'
    STORED AS TEXTFILE AS
    select c.country_name, grouped.total from
       (select sum(s.price) as total, s.countrycode as cc from
           (select price, extractCountryCode(ipaddr) as countrycode from sales) s group by countrycode) grouped
               join countries c on (c.geoname_id = grouped.cc) order by grouped.total desc limit 10;

