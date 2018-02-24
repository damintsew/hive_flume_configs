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


"США"	17541655098
NULL	6948707456
"Китай"	3742411115
"Япония"	2278635614
"Великобритания"	1369717475
"Германия"	1360686983
"Южная Корея"	1249042295
"Бразилия"	936395663
"Франция"	890462881
"Канада"	783387739

create table top_sales_by_country
    ROW FORMAT DELIMITED
    fields terminated by ','
    lines terminated by '\n'
    STORED AS TEXTFILE AS
    select c.country_name, grouped.total from
       (select sum(s.price) as total, s.countrycode as cc from
           (select price, extractCountryCode(ipaddr) as countrycode from sales) s group by countrycode) grouped
               join countries c on (c.geoname_id = grouped.cc) order by grouped.total desc limit 10;

