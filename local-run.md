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
--jars spark-bigquery-with-dependencies_2.12-0.24.2.jar,/Users/gkumargaur/.m2/repository/org/apache/spark/spark-avro_2.12/3.3.0/spark-avro_2.12-3.3.0.jar \
target/Scalapractice-1.0-SNAPSHOT.jar

##AvroToParquet
spark-submit --class com.kumargaurav.avro.AvroToParquet \
--jars spark-bigquery-with-dependencies_2.12-0.24.2.jar,gcs-connector-hadoop3-latest.jar,/Users/gkumargaur/.m2/repository/org/apache/spark/spark-avro_2.12/3.3.0/spark-avro_2.12-3.3.0.jar \
target/Scalapractice-1.0-SNAPSHOT.jar

gsutil cp target/Scalapractice-1.0-SNAPSHOT.jar gs://itd-aia-dp/pub-sub-to-bq-dataflow/jars/

gcloud dataproc jobs submit spark \
--cluster dp-dproc-standard  \
--class com.kumargaurav.avro.AvroToBigQueryTable \
--region us-central1 \
--project itd-aia-dp \
--jars "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.2.jar,gs://itd-aia-dp/pub-sub-to-bq-dataflow/jars/Scalapractice-1.0-SNAPSHOT.jar" -- "gs://itd-aia-dp-dproc-staging/solifi/ls_billing_nf-value.avsc"

##AethnaRead
spark-submit --class com.kumargaurav.athena.AthenaToCSV \
--jars spark-bigquery-with-dependencies_2.12-0.24.2.jar,AthenaJDBC42-2.0.34.1000.jar,gcs-connector-hadoop3-latest.jar \
target/Scalapractice-1.0-SNAPSHOT.jar


##PeopleAi
spark-submit --class com.kumargaurav.csv.PeopleAI \
--jars spark-bigquery-with-dependencies_2.12-0.24.2.jar \
target/Scalapractice-1.0-SNAPSHOT.jar


