#!/bin/bash

#copy dataset file Hotel_reviews.csv in shared folder

#mariadb(mysql) code 

mysql -u root -p

password

use test; #switching to the database

create table hotel_reviews (id integer, hotel_country varchar(50), review_month integer, review_year integer, average_score float, hotel_name varchar(50), reviewer_nationality varchar(50), negative_word varchar(50), no_of_reviews integer, reviewer_score float, days_since_review integer, trip_type varchar(50),customer_type varchar(50), night_stay integer)

load data local infile '~/shared/Hotel_reviews.csv' into table hotel_reviews fields terminated by ','; #loading the dataset into the table

select * from hotel_reviews; #displaying all the data from the table

select * from hotel_reviews limit 10; #displaying first 10 rows from the table

#displaying how many nationality people came to that country for visit 
select hotel_country, reviewer_nationality, count(reviewer_nationality) from hotel_reviews where hotel_country != 'UK' group by hotel_country limit 5);

#displaying how many hotels got bad reviews from customer who came to that country for visit 
select hotel_country, max(hotel_name), max(hotel_reviews.negative_word) from hotel_reviews group by hotel_country order by max(hotel_reviews.negative_word) desc;

#displaying people visiting to each country by year 
select hotel_country as Country, hotel_name as Hotel, sum(case when review_year=2015 then no_of_reviews end)as year_2015, sum(case when review_year=2016 then no_of_reviews end)as year_2016, sum(case when review_year=2017 then no_of_reviews end)as year_2017 from hotel_reviews group by hotel_country desc;

#displaying how many visitor came to country for business and leisure trip
select hotel_country as Country, hotel_name as Hotel, sum(case when trip_type='Business_Trip' then no_of_reviews end)as Business_Trip, sum(case when trip_type='Leisure_Trip' then no_of_reviews end)as Leisure_Trip from hotel_reviews group by hotel_country limit 5;

#displaying hotels which are having overall more than 9 scores with their county names
select hotel_country, hotel_name, average_score from hotel_reviews where reviewer_score>9 group by hotel_country limit 5;

#displaying hotel names and average score for each hotel whether which hotel got more than 9 score
select hotel_name, average_score from hotel_reviews where reviewer_score>9 AND hotel_country='UK' group by average_score dec limit 5;

# displaying all hotels except UK who are having more than 8 scores
select count(*) from hotel_review where reviewer_score > 8 AND hotel_country != 'UK' group by hotel_name;

#displaying hotel names in UK which got more than 9 score
select hotel_name, reviewer_score from hotel_reviews where hotel_country = 'UK' AND reviewer_score > 9;

exit;


#starting hadoop services
/usr/local/hadoop/sbin/start-all.sh

#moving dataset the into apache hive from mariadb using sqoop 

cd /usr/local/sqoop 

sqoop import --connect jdbc:mysql://127.0.0.1/test --username root --password password --table hotel_review --hive-import #importing the table into hive in the database... while importing into hive sqoop first imports it into hdfs and then hive in the default database

cd /usr/local/hive #change directory to hive


#HIVE

#running queries with storing Output with .txt output file


hive -e "select hotel_country, reviewer_nationality, count(reviewer_nationality) from hotel_reviews where hotel_country != 'UK' group by hotel_country limit 5);" > ~/shared/output/output1.txt

hive -e "select hotel_country, max(hotel_name), max(hotel_reviews.negative_word) from hotel_reviews group by hotel_country order by max(hotel_reviews.negative_word) desc;" > ~/shared/output/output2.txt

hive -e "select hotel_country as Country, hotel_name as Hotel, sum(case when review_year=2015 then no_of_reviews end)as year_2015, sum(case when review_year=2016 then no_of_reviews end)as year_2016, sum(case when review_year=2017 then no_of_reviews end)as year_2017 from hotel_reviews group by hotel_country desc;" > ~/shared/output/output3.txt

hive -e "select hotel_country as Country, hotel_name as Hotel, sum(case when trip_type='Business_Trip' then no_of_reviews end)as Business_Trip, sum(case when trip_type='Leisure_Trip' then no_of_reviews end)as Leisure_Trip from hotel_reviews group by hotel_country limit 5;" > ~/shared/output/output4.txt

hive -e "select hotel_country, hotel_name, average_score from hotel_reviews where reviewer_score>9 group by hotel_country limit 5;" > ~/shared/output/output5.txt

hive -e "select hotel_name, average_score from hotel_reviews where reviewer_score>9 AND hotel_country='UK' group by average_score dec limit 5;" > ~/shared/output/output6.txt

hive -e "select count(*) from hotel_review where reviewer_score > 8 AND hotel_country != 'UK' group by hotel_name;" > ~/shared/output/output7.txt

hive -e "select hotel_name, reviewer_score from hotel_reviews where hotel_country = 'UK' AND reviewer_score > 9;" > ~/shared/output/output8.txt



