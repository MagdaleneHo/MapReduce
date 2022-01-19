/*PigLatin codes*/

/*OBJECTIVE 1*/

ratings1 = LOAD '/home/hadoop/Assignment/ratings.csv' USING PigStorage(',') as (movieid:int,rating:int,title:chararray,budget:long);

avg_rating = FOREACH(GROUP ratings1 BY (movieid,budget)) GENERATE FLATTEN(group) AS (title,budget), AVG(ratings1.rating) AS ratingavg;
order_data = ORDER avg_rating BY budget DESC;
top20= LIMIT order_data 20;
DUMP top20;


/*OBJECTIVE 2*/
/*2017*/

ratings_2017 = LOAD '/home/hadoop/Assignment/obj1.csv' USING PigStorage(',') as (movieid:int,rating:int);

count_by_rating = FOREACH ratings_2017 GENERATE rating;
grouped_by_rating = GROUP count_by_rating by rating; 
result= FOREACH grouped_by_rating GENERATE group, COUNT(count_by_rating) AS totalrating;
order_data = ORDER result BY totalrating DESC;
DUMP order_data;

/*2019*/

ratings_2019 = LOAD '/home/hadoop/Assignment/obj2.csv' USING PigStorage(',') as (movieid:int,rating:int);

count_by_rating = FOREACH ratings_2019 GENERATE rating;
grouped_by_rating = GROUP count_by_rating by rating; 
result= FOREACH grouped_by_rating GENERATE group, COUNT(count_by_rating) AS totalrating;
order_data = ORDER result BY totalrating DESC;
DUMP order_data;


/*OBJECTIVE 3*/

ratings3 = LOAD '/home/hadoop/Assignment/ratings.csv' USING PigStorage(',') as (movieid:int,rating:int,title:chararray,budget:long);

/*Average ratings a movie has in descending order (Top 10)*/

grouped_by_title= GROUP ratings3 BY title;
avg_rating = FOREACH grouped_by_title GENERATE group as title, AVG(ratings3.rating) AS ratingavg;
order_data = ORDER avg_rating BY ratingavg DESC;
top10= LIMIT order_data 10;
DUMP top10;

/*Average ratings a movie has in no order and no limit*/

grouped_by_title= GROUP ratings3 BY title;
avg_rating = FOREACH grouped_by_title GENERATE group as title, AVG(ratings3.rating) AS ratingavg;
DUMP avg_rating;


/*OBJECTIVE 4*/

ratings = LOAD '/home/hadoop/Assignment/ratings.csv' USING PigStorage(',') as (movieid:int,rating:int,title:chararray,budget:long);

grouped_by_title= GROUP ratings BY title;
count_rating = FOREACH grouped_by_title GENERATE group as title, COUNT(ratings.rating) AS ratingcount;
order_data = ORDER count_rating BY ratingcount DESC;
top20= LIMIT order_data 20;
DUMP top20;