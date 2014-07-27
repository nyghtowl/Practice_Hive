 // connect to use hive over hadoop
 hadoop@domU-12-31-39-07-D214:~$ hive \
    -d SAMPLE=s3://elasticmapreduce/samples/hive-ads \
    -d DAY=2009-04-13 -d HOUR=08 \
    -d NEXT_DAY=2009-04-13 -d NEXT_HOUR=09 \
    -d OUTPUT=s3://<mybucket>/samples/output
  hive>

// Add json serde in S3 - this will interpret 
ADD JAR ${SAMPLE}/libs/jsonserde.jar ;

// declare table
ADD JAR ${SAMPLE}/libs/jsonserde.jar ;

// make impression table

// recover 1 partition
ALTER TABLE impressions ADD PARTITION (dt='2009-04-13-08-05') ;

// recover all partitions
ALTER TABLE impressions RECOVER PARTITIONS ; // ~1000000

// describe summary of table
describe impressions // dt column with 13 rows

// count distinct values for dt
select count(distinct dt) from impressions; // ~200

// make clicks table
CREATE EXTERNAL TABLE clicks (
impression_id string
)
PARTITIONED BY (dt string)
ROW FORMAT 
SERDE 'com.amazon.elasticmapreduce.JsonSerde'
WITH SERDEPROPERTIES ( 'paths'='impressionId' )
LOCATION '${SAMPLE}/tables/clicks' ;

ALTER TABLE clicks RECOVER PARTITIONS ;

describe clicks // dt col and 7 rows


// create temp joined_impressions table in s3

CREATE EXTERNAL TABLE joined_impressions (
requestBeginTime string, ad_id string, impression_id string, referrer string, 
  user_agent string, user_cookie string, ip string, clicked Boolean
)
PARTITIONED BY (day string, hour string)
STORED AS SEQUENCEFILE
LOCATION '${OUTPUT}/joined_impressions';


// temp store the impressions table
create external table tmp_impressions (requestBeginTime string, ad_id string, impression_id string, referrer string, 
  user_agent string, user_cookie string, ip string, dt string)
stored as SEQUENCEFILE;

// insert impressions into temp just for small subset
INSERT OVERWRITE TABLE tmp_impressions 
  SELECT 
    from_unixtime(cast((cast(i.requestBeginTime as bigint) / 1000) as int)) requestBeginTime, 
    i.ad_id, i.impression_id, i.referrer, i.user_agent, i.user_cookie, i.ip, i.dt
  FROM 
    impressions i
  WHERE 
    i.dt >= '${DAY}-${HOUR}-00' and i.dt < '${NEXT_DAY}-${NEXT_HOUR}-00'
;

// create tmp click table
create external table tmp_clicks (impression_id string)
stored as SEQUENCEFILE;

// insert impression ids for clicks for the timeframe covered plus 20 min
insert OVERWRITE table tmp_clicks
    select
        i.impression_id
    from
        impressions i
    WHERE
        i.dt >= '${DAY}-${HOUR}-00' and i.dt < '${NEXT_DAY}-${NEXT_HOUR}-20';

// join the clicks and impression temp tables into joined impressions for only ids that are not in clicks

    insert overwrite table joined_impressions partition (day='${DAY}', hour='${HOUR}')
    select i.requestBeginTime string, i.ad_id string, i.impression_id string, i.referrer string, i.user_agent string, i.user_cookie string, i.ip string, (c.impression_id is not null) clicked
    from tmp_impressions i 
    left outer join tmp_clicks c
    on i.impression_id = c.impression_id;

// declare external table
 CREATE EXTERNAL TABLE IF NOT EXISTS joined_impressions (
  request_begin_time string, ad_id string, impression_id string, 
  page string, user_agent string, user_cookie string, ip_address string,
  clicked boolean 
 )
 PARTITIONED BY (day STRING, hour STRING)
 STORED AS SEQUENCEFILE
 LOCATION '${SAMPLE}/tables/joined_impressions';


 ALTER TABLE joined_impressions RECOVER PARTITIONS;


 // apply script to clean up columns - map int
SELECT concat('ua:', trim(lower(temp.feature))) as feature, temp.ad_id, temp.clicked from (
  MAP 
   joined_impressions.user_agent, joined_impressions.ad_id, 
   joined_impressions.clicked
  USING 
   '${SAMPLE}/libs/split_user_agent.py' AS 
   feature, ad_id, clicked
  FROM 
    joined_impressions
    ) temp
  LIMIT 10;

// normalize the ip address 
  SELECT 
    concat('ip:', regexp_extract(ip, '^([0-9]{1,3}\.[0-9]{1,3}).*', 1)) AS 
      feature, ad_id, clicked
  FROM 
    joined_impressions
  LIMIT 10;

// url extraction

  SELECT concat('page:', lower(referrer)) as feature, ad_id, clicked
  FROM joined_impressions
  LIMIT 10;




// combining features
 SELECT *
  FROM (
    SELECT concat('ua:', trim(lower(ua.feature))) as feature, ua.ad_id, ua.clicked
    FROM (
      MAP joined_impressions.user_agent, joined_impressions.ad_id, joined_impressions.clicked
      USING '${SAMPLE}/libs/split_user_agent.py' as (feature STRING, ad_id STRING, clicked BOOLEAN)
      FROM joined_impressions
    ) ua

   UNION ALL

   SELECT concat('ip:', regexp_extract(ip, '^([0-9]{1,3}\.[0-9]{1,3}).*', 1)) as feature, ad_id, clicked
   FROM joined_impressions

   UNION ALL

   SELECT concat('page:', lower(referrer)) as feature, ad_id, clicked
     FROM joined_impressions
   ) temp
   order by feature desc
   limit 50;

// create feature index click table
create external table feature_index (feature STRING, ad_id STRING, clicked_percent DOUBLE)
stored as SEQUENCEFILE;

// insert union tables into feature table and group by feature and add id and calc the percent of clicks given feature
  INSERT OVERWRITE TABLE feature_index
    SELECT
      temp.feature,
      temp.ad_id,
      sum(if(temp.clicked, 1, 0)) / cast(count(1) as DOUBLE) as clicked_percent
    FROM (
      SELECT concat('ua:', trim(lower(ua.feature))) as feature, ua.ad_id, ua.clicked
      FROM (
        MAP joined_impressions.user_agent, joined_impressions.ad_id, joined_impressions.clicked
        USING '${SAMPLE}/libs/split_user_agent.py' as (feature STRING, ad_id STRING, clicked BOOLEAN)
      FROM joined_impressions
    ) ua
    
    UNION ALL
    
    SELECT concat('ip:', regexp_extract(ip, '^([0-9]{1,3}\.[0-9]{1,3}).*', 1)) as feature, ad_id, clicked
    FROM joined_impressions
    
    UNION ALL=
    
    SELECT concat('page:', lower(referrer)) as feature, ad_id, clicked
    FROM joined_impressions
  ) temp
  GROUP BY temp.feature, temp.ad_id;

// apply heuristic

  SELECT 
    ad_id, -sum(log(if(0.0001 > clicked_percent, 0.0001, clicked_percent))) AS value
  FROM 
    feature_index
  WHERE 
    feature = 'ua:safari' OR feature = 'ua:chrome'
  GROUP BY 
    ad_id
  ORDER BY 
    value ASC-
  LIMIT 100
  ;


Results

==-  -0.0
05D8MKmTtk5rd6I3HLUgaWKd9lsGup  -0.0
06sJVbSkJNqXfEnxTRVrW6r9oqHpQ8  -0.0
073DUGbHBmUOpfm0WWEoORW30mOOX6  -0.0
08tOajiLVw2x115gUp5oXthD6jLOoR  -0.0
09tkspRaCqqAKhMBTv2X8aDgG44pmu  -0.0
0AI5UbEMgfw6iiXCBhmk2v4FoiklWd  -0.0
0BemdqoLR8xB3cl4PcWF3x3xVeQbD6  -0.0
0CEoX9Xc5kM9Grq1pBwFoAVBuoLTAb  -0.0
0CFO3fE8kRbkExh0HrCX5VSsiVwrFE  -0.0
0Cqkc5VEIvjIhihB4E8t8E77Lp1EOp  -0.0
0GEUPDVt5IO8mIQckLeiEB7DpvJJxl  -0.0
0HnHLlcJWRBI58AftUWC5XoRRHvnwb  -0.0
0JF3P6kWLUtTWXmMUq0iwjTRX3x0Pg  -0.0
0TFoLAPksNFToFL8SJJtI2NSg1BHAT  -0.0
0U0ReNboLmQvrBxptAa43HQPH0D1b1  -0.0
0V2C5hFm35Wmqn4RFRISIMgcJrmGin  -0.0
0VlrxPeorDFEvUxFSdFIV8dgTGjd3G  -0.0
0XdupntWrgxntQlSePf4uxWT4JMeA3  -0.0
0aaCn8lmgPKiJVGFrB2J6FBGPxkapC  -0.0
0andaSOrUu6TgSe7iBrcqlGnJHQspJ  -0.0
0blL450X9Ud3roiLUUX3s4nHiXev4o  -0.0
0dEar84cA7wPFkIr8FmkffMd4jKnso  -0.0
0dnO64wA1ktLJ0gqioc2OTIrpjLDPr  -0.0
0fjQFLCKl8W7GSF3bEkWtVreoQKLxA  -0.0
0gEdekDBmXhpxhwwpAiN3GtgTcUNhU  -0.0
0mADOKcEAC9HXQuFSvbiME4h2tmRxN  -0.0
0nI1kLhoskdQktbaKePJQvIjaCQjMF  -0.0
0q47qUFM3fddRvIbkoGDHHhojr7VCp  -0.0
0qLWuww6UjFti6XrNx9ND82C9c2SSe  -0.0


