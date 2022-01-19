--HiveQL codes

--Table for Objective 1,3,4

CREATE TABLE IF NOT EXISTS ratings1 (movieid int,rating int, title string, budget float)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '/home/hadoop/Assignment/ratings.csv' INTO TABLE ratings1;


--OBJECTIVE 1

SELECT movieid, CAST(budget as decimal(11,2)), AVG(rating) as avgrating
FROM ratings1
GROUP by movieid, budget
ORDER BY budget DESC limit 20;



--OBJECTIVE 2

--2017

CREATE TABLE IF NOT EXISTS ratings2_1 (movieid int,rating int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH'/home/hadoop/Assignment/obj1.csv' INTO TABLE ratings2_1;

SELECT rating, COUNT(rating)
FROM ratings2_1
GROUP BY rating
ORDER BY rating DESC;

--2019
CREATE TABLE IF NOT EXISTS ratings2_2 (movieid int,rating int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH'/home/hadoop/Assignment/obj2.csv' INTO TABLE ratings2_2;

SELECT rating, COUNT(rating)
FROM ratings2_2
GROUP BY rating
ORDER BY rating DESC;


--OBJECTIVE 3

--Average ratings a movie has in descending order (Top 10)

SELECT title, AVG(rating) AS ratingavg
FROM ratings1
GROUP BY title
ORDER BY ratingavg DESC limit 10;

--Average ratings a movie has in no order and no limit

SELECT title, AVG(rating) AS ratingavg 
FROM ratings1
GROUP BY title;


--OBJECTIVE 4

SELECT title, COUNT(rating) AS ratingcount
FROM ratings1
GROUP BY title
ORDER BY ratingcount DESC limit 20;
