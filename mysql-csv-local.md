gsutil cp target/Scalapractice-1.0-SNAPSHOT.jar gs://itd-aia-de/temp/Scalapractice-1.0-SNAPSHOT.jar

//sample -> sample_event
gcloud dataproc jobs submit spark \
--cluster de-dproc-standard  \
--class com.kumargaurav.csv.virus.SampleEventCSVToTable \
--region us-central1 \
--project itd-aia-de \
--jars "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.2.jar,gs://itd-aia-de/temp/Scalapractice-1.0-SNAPSHOT.jar" -- "gs://itd-aia-de/temp/it-slr-historical-data-qa/sample.txt"

//sample table load
insert into `itd-aia-datalake.virus_landing.sample_event` SELECT pubsub_attribute_metadata,retry_attempt_number,record_ingestion_time,cast (filetype as numeric),sha256,cast ( mid as numeric),cast (source as numeric),type,analysis,platform,update_date,sha1,cast (size as numeric),family,create_date,md5,cast (status as numeric),action,metadata FROM `itd-aia-datalake.virus_landing.sample_event_fullload`;

//sig_sample ->sig_sample_event
gcloud dataproc jobs submit spark \
--cluster de-dproc-standard  \
--class com.kumargaurav.csv.virus.SigSampleCSVToTable \
--region us-central1 \
--project itd-aia-de \
--jars "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.2.jar,gs://itd-aia-de/temp/Scalapractice-1.0-SNAPSHOT.jar" -- "gs://itd-aia-de/temp/it-slr-historical-data-qa/sig_sample.txt"

insert into `itd-aia-datalake.virus_landing.sig_sample_event` SELECT pubsub_attribute_metadata,
retry_attempt_number,
record_ingestion_time,
cast(mid as numeric),
cast(id as numeric),
cast(sid as numeric),
action,
metadata FROM `itd-aia-datalake.virus_landing.sig_sample_event_fullload`

//signature -> sig_event
gcloud dataproc jobs submit spark \
--cluster de-dproc-standard  \
--class com.kumargaurav.csv.virus.SigEventCSVToTable \
--region us-central1 \
--project itd-aia-de \
--jars "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.2.jar,gs://itd-aia-de/temp/Scalapractice-1.0-SNAPSHOT.jar" -- "gs://itd-aia-de/temp/it-slr-historical-data-qa/signature.txt"

insert into `itd-aia-datalake.virus_landing.sig_event` SELECT pubsub_attribute_metadata,retry_attempt_number,record_ingestion_time,cast (severity as numeric),cast (min_version as numeric),cast (build_version as numeric),cast(type as numeric),update_date,cast(sid as numeric),cast (max_version as numeric),xml,cast(wildfire as numeric),name,category,create_date,cast(status as numeric),cast(regression_time as numeric),action,metadata FROM `itd-aia-datalake.virus_landing.sig_event_fullload`




