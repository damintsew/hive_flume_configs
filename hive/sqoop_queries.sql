select category, count(category) as counted from sales group by category order by counted DESC LIMIT 10;

create database adamintsev;
use adamintsev;

create table top_purchased_categories (category VARCHAR(100), quantity INTEGER) ENGINE=InnoDB DEFAULT CHARSET=utf8;
INSERT INTO top_purchased_categories (category, quantity) VALUES (?,?)

create table top_products_for_categories (category VARCHAR(150), product_name VARCHAR(250)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
INSERT INTO top_products_for_categories (category, product_name) VALUES (?,?)

create table top_sales_by_country (country VARCHAR(150), sales BIGINT) ENGINE=InnoDB DEFAULT CHARSET=utf8;
INSERT INTO top_sales_by_country (country, sales) VALUES (?,?)



sqoop export --connect "jdbc:mysql://ip-10-0-0-21:3306/adamintsev" \
--username root --password cloudera \
--table top_purchased_categories \
--export-dir /user/hive/warehouse/adamintsev.db/top_purchased_categories \
--input-fields-terminated-by ',' \
--input-lines-terminated-by '\n'

sqoop export --connect "jdbc:mysql://ip-10-0-0-21:3306/adamintsev" \
--username root --password cloudera \
--table top_products_for_categories \
--export-dir /user/hive/warehouse/adamintsev.db/top_products_for_categories \
--input-fields-terminated-by ',' \
--input-lines-terminated-by '\n'

sqoop export --connect "jdbc:mysql://ip-10-0-0-21:3306/adamintsev" \
--username root --password cloudera \
--table top_sales_by_country \
--export-dir /user/hive/warehouse/adamintsev.db/top_sales_by_country \
--input-fields-terminated-by ',' \
--input-lines-terminated-by '\n'