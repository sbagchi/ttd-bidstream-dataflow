# ttd-bidstream-dataflow
Dataflow pipeline for ttd-bidstream using Apache Beam

# Run on GCP
##setup credentials for Apapche beam job
export GOOGLE_APPLICATION_CREDENTIALS="/Users/sbagchi/Documents/gcp-test-drive/dataflow/Dataflow-Quick-Start-d2bec614e306.json"

export PROJECT=$(gcloud info --format='value(config.project)')
export REGION=us-central1
export ZONE=us-central1-a
gcloud config set compute/zone ${ZONE}

### create input data bucket and copy the input file
gsutil mb -c standard -l us-central1 gs://dataflow-ttd-bid-input
gsutil cp /Users/sbagchi/Documents/dstillery/idea-workspace/ttd-bidstream-dataflow/examples/avails.csv gs://dataflow-ttd-bid-input

### create output data bucket
gsutil mb -c standard -l ${REGION} gs://dataflow-ttd-bid-output

### create hive staging data bucket
gsutil mb -c standard -l ${REGION} gs://dataproc-ttd-bid-staging

### run dataflow job (ETL pipeline)
use numshards 0 to automatically scale
mvn -Denforcer.skip=true compile exec:java \
-Dexec.mainClass=com.dstillery.dataflow.bidstream.ttd.BidstreamProcessingPipeline \
-Dexec.args=" \
--jobName=bidstream-pipeline2 \
--project=dst-datawarehouse \
--region=us-central1 \
--experiments=shuffle_mode=service \
--runner=DataflowRunner \
--useGcsSource=false \
--workerMachineType=n1-highmem-4 \
--numWorkers=5 \
--maxNumWorkers=100 \
--inputFile=gs://dst-ttd-bidstream-raw/2020/07/01/00/*.log.gz \
--output=gs://dst-dataflow-output2/output/YYYY-MM-DD \
--stagingLocation=gs://dst-dataflow-output2/staging \
--avroTempDirectory=gs://dst-dataflow-output2/temp" \
-Pdataflow-runner  

# run locally with TTD files     
mvn clean compile exec:java \
      -Dlog4j.configuration=file:./examples/log4j.xml \
      -Dexec.mainClass=com.dstillery.dataflow.bidstream.ttd.BidstreamProcessingPipeline \
      -Dexec.args="--inputFile=examples/sample*.log.gz \
      --output=target/output/YYYY-MM-DD \
      --avroTempDirectory=target/output \
      --useGcsSource=false " -Pdirect-runner 
      
# inspect local file
parquet-tools cat target/output/date_z=20200423/hour_z=00/20200423000000-20200423010000-0-1.parquet

# check parquet schema
parquet-tools schema target/output/date_z=20200423/hour_z=00/20200423000000-20200423010000-0-1.parquet       