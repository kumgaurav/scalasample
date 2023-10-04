gcloud dataproc jobs submit spark \
--cluster dp-dproc-standard  \
--class com.kumargaurav.athena.AthenaToCSV \
--region us-central1 \
--project itd-aia-dp \
--jars "gs://itd-aia-dp/pub-sub-to-bq-dataflow/jars/AthenaJDBC42-2.0.34.1000.jar,gs://itd-aia-dp/pub-sub-to-bq-dataflow/jars/Scalapractice-1.0-SNAPSHOT.jar"


##AethnaRead
spark-submit --class com.kumargaurav.athena.AthenaToCSV \
--jars spark-bigquery-with-dependencies_2.12-0.24.2.jar,AthenaJDBC42-2.0.34.1000.jar,gcs-connector-hadoop3-latest.jar \
target/Scalapractice-1.0-SNAPSHOT.jar \
org_200000038.accounts id 366137
org_200000038.activities lead_id 71153254
org_200000038.activity_participants id 173152574

##Activities
spark-submit --class com.kumargaurav.athena.AthenaActivitiesToCSV \
--jars spark-bigquery-with-dependencies_2.12-0.24.2.jar,AthenaJDBC42-2.0.34.1000.jar,gcs-connector-hadoop3-latest.jar \
target/Scalapractice-1.0-SNAPSHOT.jar \
org_200000038.activities lead_id 72647778

##Participants
spark-submit --class com.kumargaurav.athena.AthenaParticipantsToCSV \
--jars spark-bigquery-with-dependencies_2.12-0.24.2.jar,AthenaJDBC42-2.0.34.1000.jar,gcs-connector-hadoop3-latest.jar \
target/Scalapractice-1.0-SNAPSHOT.jar \
org_200000038.activity_participants id 174962388

passowrd=mypassword
