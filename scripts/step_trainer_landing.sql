CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer_landing (
    sensorreadingtime BIGINT,
    serialnumber STRING,
    distancefromobject INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'FALSE',
    'dots.in.keys' = 'FALSE',
    'case.insensitive' = 'TRUE',
    'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-lakehouse-alexander-harvey-d609-project/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');