

CREATE DATABASE IF NOT EXISTS covid_19_lookup;
CREATE DATABASE IF NOT EXISTS covid_19_raw;
CREATE DATABASE IF NOT EXISTS covid_19_processed;

DROP TABLE IF EXISTS covid_19_lookup.dim_date;

CREATE EXTERNAL TABLE IF NOT EXISTS covid_19_lookup.dim_date (
   date_key INT,
   the_date DATE ,
   the_year INT,
   the_month INT,
   the_day INT,   
   day_name STRING,   
   day_of_year BIGINT,
   week_of_month BIGINT,
   week_of_year BIGINT,
   month_name STRING,
   year_month INT,
   year_week INT
   )
   ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
   STORED AS TEXTFILE
   LOCATION 'abfs://lookup@covid19dlake.dfs.core.windows.net/dim_date/'
   TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS covid_19_lookup.dim_country;

CREATE EXTERNAL TABLE IF NOT EXISTS covid_19_lookup.dim_country (
   country STRING,
   country_code_2_digit STRING ,
   country_code_3_digit STRING
   )
   ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
   STORED AS TEXTFILE
   LOCATION 'abfs://lookup@covid19dlake.dfs.core.windows.net/dim_country/'
   TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS covid_19_raw.testing;

CREATE EXTERNAL TABLE IF NOT EXISTS covid_19_raw.testing (
   country  STRING,
   country_code   STRING,
   year_week   STRING,
   new_cases   BIGINT,
   tests_done  BIGINT,
   population  BIGINT, 
   testing_rate   DOUBLE,
   positivity_rate   DOUBLE,
   testing_data_source  STRING
)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
   STORED AS TEXTFILE
   LOCATION 'abfs://raw@covid19dlake.dfs.core.windows.net/europe_data/testing/'
   TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS covid_19_processed.testing;

CREATE TABLE IF NOT EXISTS covid_19_processed.testing (
   country  STRING,
   country_code_2_digit  STRING,
   country_code_3_digit STRING,
   year_week   STRING,
   week_start_date STRING,
   week_end_date STRING,
   new_cases   BIGINT,
   tests_done  BIGINT,
   population  BIGINT, 
   testing_rate   DOUBLE,
   positivity_rate   DOUBLE,
   testing_data_source  STRING
)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
   STORED AS TEXTFILE
   LOCATION 'abfs://processed@covid19dlake.dfs.core.windows.net/europe_data/testing'
   TBLPROPERTIES ("skip.header.line.count"="1");

INSERT OVERWRITE TABLE covid_19_processed.testing
SELECT  t.country  ,
      c.country_code_2_digit   ,
      c.country_code_3_digit   ,
      t.year_week   ,
      MIN(d.the_date) AS week_start_date, 
      MAX(d.the_date) AS week_end_date ,
      t.new_cases   ,
      t.tests_done  ,
      t.population  , 
      t.testing_rate   ,
      t.positivity_rate   ,
      t.testing_data_source  
FROM covid_19_raw.testing t 
JOIN covid_19_lookup.dim_date d 
   ON (t.year_week = CONCAT(CONCAT(d.the_year, '-W'),  LPAD(d.week_of_year, 2, '0')))
JOIN covid_19_lookup.dim_country c
   ON (t.country_code = c.country_code_2_digit)
GROUP BY t.country  ,
      c.country_code_2_digit   ,
      c.country_code_3_digit   ,
      t.year_week   ,
      t.new_cases   ,
      t.tests_done  ,
      t.population  , 
      t.testing_rate   ,
      t.positivity_rate   ,
      t.testing_data_source  ;
        