-- creating a database
CREATE DATABASE my_imdb;

-- selecting the database
USE my_imdb;

-- creating table title_akas
CREATE TABLE title_akas
(
    titleId STRING,
    ordering INT,
    title STRING,
    region STRING,    
    language STRING,
    types array<STRING>,
    attributes array<STRING>,
    isOriginalTitle BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

LOAD DATA INPATH '/title.akas.tsv'
INTO TABLE title_akas;

SELECT * FROM title_akas LIMIT 5;

SELECT region, COUNT(*) FROM title_akas GROUP BY region;

--creating title_akas_buckets partionned by language clustered by title id
USE my_imdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.bucketing = true;

CREATE TABLE title_akas_buckets
(
    titleId STRING,
    ordering INT,
    title STRING,
    region STRING,
    types array<STRING>,
    attributes array<STRING>,
    isOriginalTitle BOOLEAN
)

PARTITIONED BY (language STRING)
CLUSTERED BY(titleId) INTO 16 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into title_akas_buckets
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE title_akas_buckets PARTITION (language)
      SELECT  titleId,
              ordering,
              title,
              region,
              types,
              attributes,
              isOriginalTitle,
              language        
      FROM title_akas;


--creating title_akas_p partionned by language 
USE my_imdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.bucketing = true;

CREATE TABLE title_akas_p
(
    titleId STRING,
    ordering INT,
    title STRING,
    region STRING,
    types array<STRING>,
    attributes array<STRING>,
    isOriginalTitle BOOLEAN
)

PARTITIONED BY (language STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into title_akas_p
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE title_akas_p PARTITION (language)
      SELECT  titleId,
              ordering,
              title,
              region,
              types,
              attributes,
              isOriginalTitle,
              language        
      FROM title_akas;


--creating title_akas_pr partionned by region 
USE my_imdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.bucketing = true;

CREATE TABLE title_akas_pr
(
    titleId STRING,
    ordering INT,
    title STRING,
    language STRING,
    types array<STRING>,
    attributes array<STRING>,
    isOriginalTitle BOOLEAN
)

PARTITIONED BY (region STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into title_akas_pr
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE title_akas_pr PARTITION (region)
      SELECT  titleId,
              ordering,
              title,
              language,
              types,
              attributes,
              isOriginalTitle,
              region
                      
      FROM title_akas;


SELECT language, COUNT(*) FROM title_akas GROUP BY language;
--Time taken 1.818 sec

SELECT language, COUNT(*) FROM title_akas_buckets GROUP BY language;
--Time taken 1.733 sec

SELECT language, COUNT(*) FROM title_akas_p GROUP BY language;
--Time taken 1.387 sec

SELECT language, COUNT(*) FROM title_akas_pr GROUP BY language;
--Time taken 1.408 sec


SELECT region, COUNT(*) FROM title_akas GROUP BY region;
--Time taken 1.355 sec

SELECT region, COUNT(*) FROM title_akas_buckets GROUP BY region;
--Time taken 1.394 sec

SELECT region, COUNT(*) FROM title_akas_p GROUP BY region;
--Time taken 1.362 sec

SELECT region, COUNT(*) FROM title_akas_pr GROUP BY region;
--Time taken 1.428 sec

--Selected Table is title_akas_p because of its Time taken is the lowest

--creating title_akas_basics
CREATE TABLE title_akas_basics
(
     tconst STRING,
     titleType STRING,
     primaryTitle STRING,
     originalTitle STRING,
     isAdult BOOLEAN,
     startYear INT,
     endYear INT,
     runtimeMinutes INT,
     genres array<STRING>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into title_akas_basics
LOAD DATA INPATH '/title.basics.tsv'
INTO TABLE title_akas_basics;

SELECT genres, COUNT(*) FROM title_akas_basics GROUP BY genres;
SELECT isAdult, COUNT(*) FROM title_akas_basics GROUP BY isAdult;
SELECT startYear, COUNT(*) FROM title_akas_basics GROUP BY startYear;

--creating title_basics_buckets partionned by start year clustered by tconst into 16 buckets;
USE my_imdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

CREATE TABLE title_basics_buckets
(
     tconst STRING,
     titleType STRING,
     primaryTitle STRING,
     originalTitle STRING,
     isAdult BOOLEAN,
     endYear INT,
     runtimeMinutes INT,
     genres array<STRING>  
)
PARTITIONED BY (startYear INT)
CLUSTERED BY(tconst) INTO 16 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into title_basics_buckets 
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE title_basics_buckets PARTITION (startYear)
      SELECT  tconst,
              titleType,
              primaryTitle,
              originalTitle,
              isAdult,
              endYear,
              runtimeMinutes,
              genres, 
              startYear        
      FROM title_akas_basics;



--creating title_basics_p partionned by start year;
USE my_imdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

CREATE TABLE title_basics_p
(
     tconst STRING,
     titleType STRING,
     primaryTitle STRING,
     originalTitle STRING,
     isAdult BOOLEAN,
     endYear INT,
     runtimeMinutes INT,
     genres array<STRING>  
)
PARTITIONED BY (startYear INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into title_basics_p 
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE title_basics_p PARTITION (startYear)
      SELECT  tconst,
              titleType,
              primaryTitle,
              originalTitle,
              isAdult,
              endYear,
              runtimeMinutes,
              genres,
              startYear        
      FROM title_akas_basics;

SELECT startYear, COUNT(*) FROM title_akas_basics GROUP BY startYear;
--Time taken 1.369 sec

SELECT startYear, COUNT(*) FROM title_basics_buckets GROUP BY startYear;
--Time taken 2.67 sec

SELECT startYear, COUNT(*) FROM title_basics_p GROUP BY startYear;
--Time taken 1.487 sec

--Selected Table is table title_akas_basics because its Time taken is the lowest


--create table title_crew
CREATE TABLE title_crew
(
     tconst STRING,
     directors array<STRING>,
     writers array<STRING>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into title_crew
LOAD DATA INPATH '/title.crew.tsv'
INTO TABLE title_crew;


--creating table title_crew_buckets clustered by tconst into 16 buckets
USE my_imdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

CREATE TABLE title_crew_buckets
(
     tconst STRING,
     directors array<STRING>,
     writers array<STRING>
)

CLUSTERED BY(tconst) INTO 16 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into TABLE title_crew_buckets
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE title_crew_buckets
      SELECT  tconst,
              directors,
              writers      
      FROM title_crew;

SELECT directors, COUNT(*) FROM title_crew GROUP BY directors;
--Time taken 1.381 sec

SELECT directors, COUNT(*) FROM title_crew_buckets GROUP BY directors;
--Time taken 1.411 sec

--create table title_episode
CREATE TABLE title_episode
(
     tconst STRING,
     parentTconst STRING,
     seasonNumber INT,
     episodeNumber INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into title_episode
LOAD DATA INPATH '/title.episode.tsv'
INTO TABLE title_episode;

--creating table title_episode_buckets clustered by tconst into 16 buckets
USE my_imdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

CREATE TABLE title_episode_buckets
(
     tconst STRING,
     parentTconst STRING,
     seasonNumber INT,
     episodeNumber INT
)

CLUSTERED BY(tconst) INTO 16 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into TABLE title_episode_buckets
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE title_episode_buckets
      SELECT  tconst,
              parentTconst,
              seasonNumber,
              episodeNumber     
      FROM title_episode;

SELECT parentTconst, COUNT(*) FROM title_episode GROUP BY parentTconst;
--Time taken 1.595 sec

SELECT parentTconst, COUNT(*) FROM title_episode_buckets GROUP BY parentTconst;
--Time taken 1.318 sec

--Selected table is title_episode_buckets because because its Time taken is the lowest

--create table title_principals
CREATE TABLE title_principals
(
     tconst STRING,
     ordering INT,
     nconst STRING,
     category STRING,
     job STRING,
     characters STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into title_episode
LOAD DATA INPATH '/title.principals.tsv'
INTO TABLE title_principals;

SELECT category, COUNT (*) FROM title_principals GROUP BY category;


--creating title_principals_buckets partionned by category clustered by tconst into 16 buckets;
USE my_imdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

CREATE TABLE title_principals_buckets
(
     tconst STRING,
     ordering INT,
     nconst STRING,
     job STRING,
     characters STRING 
)
PARTITIONED BY (category STRING)
CLUSTERED BY(tconst) INTO 16 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into title_principals_buckets 
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE title_principals_buckets PARTITION (category)
      SELECT  tconst,
              ordering,
              nconst,
              job,
              characters,
              category        
      FROM title_principals;

SELECT category, COUNT (*) FROM title_principals GROUP BY category;
--Time taken 1.651 sec

SELECT category, COUNT (*) FROM title_principals_buckets GROUP BY category;
--Time taken 1.441 sec

--Selected table is title_principals_buckets  because its Time taken is the lowest

--create table title_ratings
CREATE TABLE title_ratings
(
     tconst STRING,
     averageRating FLOAT, 
     numVotes  INT
     
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into title_ratings
LOAD DATA INPATH '/title.ratings.tsv'
INTO TABLE title_ratings;

SELECT DISTINCT averageRating FROM title_ratings;
SELECT averageRating, COUNT(*) GROUP BY averageRating;
SELECT DISTINCT tconst FROM title_ratings;

--creating table title_ratings_part partitionned by averageRating 
USE my_imdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

CREATE TABLE title_ratings_part
(
     tconst STRING, 
     numVotes  INT
)

PARTITIONED BY (averageRating FLOAT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into TABLE title_ratings_part
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE title_ratings_part PARTITION (averageRating)
      SELECT  tconst,               
              numVotes,
              averageRating    
      FROM title_ratings;


--creating table title_ratings_buckets clustered CLUSTERED BY(tconst) INTO 16 BUCKETS
USE my_imdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

CREATE TABLE title_ratings_buckets
(
     tconst STRING,
     averageRating FLOAT, 
     numVotes  INT
)

CLUSTERED BY (tconst) INTO 16 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into TABLE title_ratings_buckets
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE title_ratings_buckets
      SELECT  tconst,               
              numVotes,
              averageRating    
      FROM title_ratings;

--creating table title_ratings_buckets1 clustered CLUSTERED BY(tconst) INTO 32 BUCKETS
USE my_imdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

CREATE TABLE title_ratings_buckets1
(
     tconst STRING,
     averageRating FLOAT, 
     numVotes  INT
)

CLUSTERED BY (tconst) INTO 32 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--creating table name_basics_p partitionned by birthyear
USE my_imdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

CREATE TABLE name_basics_p
(
    nconst STRING,
    primaryName STRING,
    deathYear INT,
    primaryProfession array<STRING>,
    knownForTitles array<STRING>
)

PARTITIONED BY (birthYear INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;


SELECT averageRating, COUNT(*) FROM title_ratings GROUP BY averageRating;
--Time taken 1.333sec
SELECT averageRating, COUNT(*) FROM title_ratings_part GROUP BY averageRating;
--Time taken 1.496 sec

SELECT averageRating, COUNT(*) FROM title_ratings_buckets GROUP BY averageRating;
--Time taken 1.323 sec

SELECT averageRating, COUNT(*) FROM title_ratings_buckets1 GROUP BY averageRating;
--Time taken 1.362 sec

--Selected table is title_ratings_buckets because its Time taken is the lowest

--create table name_basics
CREATE TABLE name_basics
(
    nconst STRING,
    primaryName STRING,
    birthYear INT,
    deathYear INT,
    primaryProfession array<STRING>,
    knownForTitles array<STRING>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into name_basics
LOAD DATA INPATH '/name.basics.tsv'
INTO TABLE name_basics;

--creating table name_basics_p partitionned by birthyear
USE my_imdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

CREATE TABLE name_basics_p
(
    nconst STRING,
    primaryName STRING,
    deathYear INT,
    primaryProfession array<STRING>,
    knownForTitles array<STRING>
)

PARTITIONED BY (birthYear INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into TABLE name_basics_p
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE name_basics_p PARTITION (birthYear)
      SELECT  nconst,
              primaryName,
              deathYear,
              primaryProfession,
              knownForTitles,
              birthYear
      FROM name_basics;

--creating table name_basics_b clustered by nconst
USE my_imdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

CREATE TABLE name_basics_b
(
    nconst STRING,
    primaryName STRING,
    birthYear INT,
    deathYear INT,
    primaryProfession array<STRING>,
    knownForTitles array<STRING>
)

CLUSTERED BY (nconst) INTO 16 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
ESCAPED BY "\\"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

--inserting data into TABLE name_basics_b
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE name_basics_b
      SELECT  nconst,
              primaryName,
              birthYear,
              deathYear,
              primaryProfession,
              knownForTitles    
      FROM name_basics;

SELECT birthYear, COUNT(*) FROM name_basics GROUP BY birthYear;
--Time taken 1.772 sec

SELECT birthYear, COUNT(*) FROM name_basics_p GROUP BY birthYear;
--Time taken 1.758 sec

SELECT birthYear, COUNT(*) FROM name_basics_b GROUP BY birthYear;
--Time taken 1.371 sec

--Selected table is name_basics_b because its Time taken is the lowest