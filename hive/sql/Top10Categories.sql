
select category, count(category) as counted from sales group by category order by counted DESC LIMIT 10;

create table top_purchased_categories
    ROW FORMAT DELIMITED
    fields terminated by ','
    lines terminated by '\n'
    STORED AS TEXTFILE AS
    select category, count(category) as counted from sales group by category order by counted DESC LIMIT 10;



﻿Зарубежное кино (DVD)	408794
Зарубежное кино (HD Video)	403515
Фильмы 1991-2000	387742
Классика зарубежного кино	380231
Фильмы 2006-2010	373987
Фильмы до 1990 года	257385
Фильмы 2001-2005	254872
Фильмы 2011-2015	216171
Арт-хаус и авторское кино	212137
Азиатские фильмы	211693
Time taken: 183.555 seconds, Fetched: 10 row(s)
