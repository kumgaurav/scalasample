##
spark-submit --class com.kumargaurav.csv.SplitCSVOnTableColumns \
--jars spark-bigquery-with-dependencies_2.12-0.24.2.jar \
target/Scalapractice-1.0-SNAPSHOT.jar

##JSONToCSV
spark-submit --class com.kumargaurav.json.JsonToCsv \
--jars spark-bigquery-with-dependencies_2.12-0.24.2.jar \
target/Scalapractice-1.0-SNAPSHOT.jar

##AvroRead
spark-submit --class com.kumargaurav.avro.AvroReadTest \
--jars spark-bigquery-with-dependencies_2.12-0.24.2.jar \
target/Scalapractice-1.0-SNAPSHOT.jar

gsutil cp target/Scalapractice-1.0-SNAPSHOT.jar gs://itd-aia-dp/IRIndexUpdater/jars/

gcloud dataproc jobs submit spark \
--cluster dp-dproc-standard  \
--class com.kumargaurav.avro.AvroToBigQueryTable \
--region us-central1 \
--project itd-aia-dp \
--jars "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.25.2.jar,gs://itd-aia-dp/IRIndexUpdater/jars/Scalapractice-1.0-SNAPSHOT.jar" -- "gs://itd-aia-dp-dproc-staging/solifi/ls_billing_nf-value.avsc"