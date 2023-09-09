-- Athena Queries for Big Data Project - World Cup

/*
   1. Loading In Data from s3

    Created 2 tables that will be used for aggregations in Athena for this project:
    - 'football' - contains cleaned data from the original datframe
    - 'pred_full' - contains predictions data collected after Machine Learning in Databricks
 */

-- Creating table 'football'

CREATE EXTERNAL TABLE IF NOT EXISTS `project`.`football` (
  `id` bigint,
  `user_name` string,
  `user_screen_name` string,
  `text` string,
  `follower_count` int,
  `location` string,
  `created_at` string,
  `sentiment` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = '\t')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://b17-masha/project/b17-masha/project/clean/'
TBLPROPERTIES (
  'classification' = 'csv',
  'skip.header.line.count' = '1'
);

-- Creating table 'pred_full'

CREATE EXTERNAL TABLE IF NOT EXISTS `project`.`pred_full` (
  `text` string,
  `clean` string,
  `sentiment` string,
  `label` double,
  `prediction` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = '\t')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://b17-masha/project/b17-masha/project/predictions_full/'
TBLPROPERTIES (
  'classification' = 'csv',
  'skip.header.line.count' = '1'
);

/*
   2. Aggregations on football table
*/

-- 1. 'created_at' column had to be imported as string type. Here, it will be converted to timestamp and following features will be extracted for analysis:
-- month_day
-- month
-- day
-- day of week
-- hour

-- 2. The new columns will be added to existing columns in our table and a new table 'clean' will be created

CREATE TABLE clean AS
    (SELECT
    user_name,
    follower_count,
    location,
    text,
    sentiment,
    date_format(from_iso8601_timestamp(created_at), '%m-%d') AS month_day,
    extract(month from from_iso8601_timestamp(created_at)) AS month,
    extract(day from from_iso8601_timestamp(created_at)) AS day,
    extract(dow from from_iso8601_timestamp(created_at)) AS day_of_week,
    extract(hour from from_iso8601_timestamp(created_at)) AS hour
FROM football)
;

/*
   2. Aggregations on pred_full table
*/

-- 1. Creating table 'words' using a query that splits all words to and puts them in each own row
-- Free edition of QuickSight did not allow for adequate text formatting and aggregation, so this workaround was used

CREATE TABLE words AS
(SELECT word, sentiment
FROM (
    SELECT split(clean, ' ') as words,
         sentiment
    FROM pred_full
) t1
CROSS JOIN UNNEST(words) AS t2(word)
WHERE word NOT LIKE '');

-- 2. Creating table with words only for tweets labelled positive

CREATE TABLE words_pos AS
    (SELECT word, sentiment
    FROM (
          SELECT split(clean, ' ') as words,
                 sentiment
          FROM pred_full
    ) t1
CROSS JOIN UNNEST(words) AS t2(word)
WHERE word NOT LIKE ''
AND sentiment = 'positive')
;

-- 3. Creating table with words only for tweets labelled negative

CREATE TABLE words_neg AS
(SELECT word, sentiment
FROM (
    SELECT split(clean, ' ') AS words,
    sentiment
    FROM pred_full
) t1
CROSS JOIN UNNEST(words) AS t2(word)
WHERE word NOT LIKE ''
AND sentiment = 'negative')
;

-- 4. Creating table 'predictions' and adding column is_correct

CREATE TABLE predictions AS
    (SELECT *,
    CASE WHEN label = prediction THEN 'correct'
    ELSE 'incorrect'
    END AS is_correct
    FROM pred_full)
;

-- 5 Creating table 'incorrect_words' by filtering out predictions table only keeping incorrect predictions
-- Exploding words to be able to extract weights

CREATE TABLE incorrect_words AS
    (SELECT word, sentiment, label, prediction, is_correct
    FROM (
    SELECT split(clean, ' ') AS words,
    sentiment,
    label,
    prediction,
    is_correct
    FROM predictions
    ) t1
CROSS JOIN UNNEST(words) AS t2(word)
WHERE word NOT LIKE ''
AND is_correct = 'incorrect');

## Tables created to use in Athena
-- 1. clean
-- 2. words
-- 3. words_pos
-- 4. words_neg
-- 5. predictions
-- 6. incorrect_words