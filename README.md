# Taxi pipeline: recover sessions, upsert in BigQuery

This repository contains a streaming Dataflow pipeline reading data from Pubsub
and recovering the sessions from potentially unordered data, by using a common
key to all the points received for the same vehicle.

The pipeline can probably be easily adapted to any other Apache Beam runner,
but this repository assumes you are running in Google Cloud Dataflow.

## Data input

We are using here a public PubSub topic with data, so we don't need to setup our
own to run this pipeline.

The topic is `projects/pubsub-public-data/topics/taxirides-realtime`.

That topic contains messages from the NYC Taxi Ride dataset. Here is a sample of
the data contained in a message in that topic:

```json
{
"ride_id": "328bec4b-0126-42d4-9381-cb1dbf0e2432",
"point_idx": 305,
"latitude": 40.776270000000004,
"longitude": -73.99111,
"timestamp": "2020-03-27T21:32:51.48098-04:00",
"meter_reading": 9.403651,
"meter_increment": 0.030831642,
"ride_status": "enroute",
"passenger_count": 1
}
```

But the messages also contain metadata, that is useful for streaming pipelines.
In this case, the messages contain an attribute of name `ts`, which contains the
same timestamp as the field of name `timestamp` in the data. Remember that
PubSub treats the data as just a string of bytes (in topics with no schema), so
it does not _know_ anything about the data itself. The metadata fields are
normally used to publish messages with specific ids and/or timestamps.

## Data output

The goal is grouping together all the messages that belong to the
same taxi ride, and recovering the initial and end timestamps, the initial
and end status (ride_status) and calculating the duration of the trip in
seconds. We have to insert these sessions in streaming in BigQuery, doing
upserts. We want to deal with potential late data too, recalculating the
sessions if necessary.

The pipeline uses three triggers:

- An early trigger for every single message received before the watermark
- A trigger when the watermark is reached.
- A late trigger for every single late message before a certain threshold
(configurable).

So in Bigquery, you will see some sessions that are "partial" (the end
status is not `dropoff` yet, but those sessions should be all eventually complete).

In addition to this, the output is written to different tables, depending on the
first character of the session id. This is done just to show how to write to dynamic
destinations with BigQueryIO and using change-data-capture / upserts.

## gcloud authentication

You need to have a Google Cloud project with editor or owner permissions,
in order to be able to create the resources for this demo.

You need to have [the Google Cloud SDK installed](https://cloud.google.com/sdk/docs/install-sdk),
or alternatively you can use the Cloud Shell in your project.

The code snippets below set some environment variables that will be useful to
run other commands. You can use these code snippets locally or in the Cloud Shell.
Make sure that you set the right values for the variables before proceeding with
the rest of code snippets.

```shell
export YOUR_EMAIL=<WRITE YOUR EMAIL HERE>
export YOUR_PROJECT=<WRITE YOUR PROJECT ID HERE>
export GCP_REGION=<WRITE HERE YOUR GCP REGION>  # e.g. europe-southwest1
```

These are other values that you probably don't need to change:

```shell
export SUBNETWORK_NAME=default
export SUBSCRIPTION_NAME=taxis
export DATASET_NAME=data_playground
export SERVICE_ACCOUNT_NAME=taxi-pipeline-sa
export SESSIONS_TABLE=sessions
export ERRORS_TABLE=errors
```

You need to make sure that the subnetwork above (the default subnetwork in your
chosen region) has [Private Google Access enabled](https://cloud.google.com/vpc/docs/configure-private-google-access#enabling-pga).

Run the following to create a specific configuration for your Google Cloud project.
You can probably skip this if you are in the Cloud Shell.

```shell
gcloud config configurations create taxipipeline-streaming
gcloud config set account $YOUR_EMAIL
gcloud config set project $YOUR_PROJECT
```

Make sure that you are authenticated, by running

```shell
gcloud auth login
```

and

```shell
gcloud auth application-default login
```

## Required resources

### Required services

In your project you need to enable the following APIs:

```shell
gcloud services enable dataflow
gcloud services enable pubsub
gcloud services enable bigquery
```

### Bucket

You will need a GCS bucket for staging files and for temp files. We create a bucket
with the same name as the project:

```shell
gcloud storage buckets create gs://$YOUR_PROJECT --location=$GCP_REGION
```

### Pub/Sub subscription

To inspect the messages from this topic, you can create a subscription, and then
pull some messages.

To create a subscription, use the gcloud cli utility (installed by default in
the Cloud Shell). Fill this for the subscription name (for instance, `taxis`):

```shell
export TOPIC=projects/pubsub-public-data/topics/taxirides-realtime
gcloud pubsub subscriptions create $SUBSCRIPTION_NAME --topic $TOPIC
```

To pull messages:

```shell
gcloud pubsub subscriptions pull $SUBSCRIPTION_NAME --limit 3
```

or if you have `jq` installed (for pretty printing of JSON)

```shell
gcloud pubsub subscriptions pull $SUBSCRIPTION_NAME --limit 3 | grep " {" | cut -f 2 -d ' ' | jq
```

### BigQuery dataset

Choose a dataset name (for instance, `data_playground`):

And then create the dataset from the command line:

```shell
bq mk -d --data_location=$GCP_REGION $DATASET_NAME
```

### Service account

Let's now create a Dataflow worker service account, with permissions to read from
the Pub/Sub subscription and to write to BigQuery:

```shell
gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME
```

And now let's give all the required permissions:

```shell
gcloud projects add-iam-policy-binding $YOUR_PROJECT \
--member="serviceAccount:$SERVICE_ACCOUNT_NAME@$YOUR_PROJECT.iam.gserviceaccount.com" \
--role="roles/dataflow.worker"

gcloud projects add-iam-policy-binding $YOUR_PROJECT \
--member="serviceAccount:$SERVICE_ACCOUNT_NAME@$YOUR_PROJECT.iam.gserviceaccount.com" \
--role="roles/storage.admin"

gcloud projects add-iam-policy-binding $YOUR_PROJECT \
--member="serviceAccount:$SERVICE_ACCOUNT_NAME@$YOUR_PROJECT.iam.gserviceaccount.com" \
--role="roles/pubsub.editor"

gcloud projects add-iam-policy-binding $YOUR_PROJECT \
--member="serviceAccount:$SERVICE_ACCOUNT_NAME@$YOUR_PROJECT.iam.gserviceaccount.com" \
--role="roles/pubsub.subscriber"

gcloud projects add-iam-policy-binding $YOUR_PROJECT \
--member="serviceAccount:$SERVICE_ACCOUNT_NAME@$YOUR_PROJECT.iam.gserviceaccount.com" \
--role="roles/bigquery.dataEditor"
```

## Build

To create a FAT jar, ready to be deployed in Dataflow without additional
dependencies, run the following command:

```shell
./gradlew build
```

## Test

Run the following command:

```shell
./gradlew test
```

## Run

Make sure that you have followed the steps above, and you are authenticated and
have created the input subscription and the output BigQuery datasets, prior to
running the pipeline.

Make also sure that you have Java >=8 or <=17 installed in your machine.
Check your current version with:

```shell
java -version
```

And then run the pipeline:

```shell
TEMP_LOCATION=gs://$YOUR_PROJECT/tmp
SUBSCRIPTION=projects/$YOUR_PROJECT/subscriptions/$SUBSCRIPTION_NAME
NETWORK=regions/$GCP_REGION/subnetworks/$SUBNETWORK_NAME
SERVICE_ACCOUNT=$SERVICE_ACCOUNT_NAME@$YOUR_PROJECT.iam.gserviceaccount.com

./gradlew run -Pargs="
--pipeline=taxi-sessions \
--runner=DataflowRunner \
--project=$YOUR_PROJECT \
--region=$GCP_REGION \
--tempLocation=$TEMP_LOCATION \
--usePublicIps=false \
--serviceAccount=$SERVICE_ACCOUNT \
--subnetwork=$NETWORK \
--enableStreamingEngine \
--rideEventsSubscription=$SUBSCRIPTION \
--destinationDataset=$DATASET_NAME \
--sessionsDestinationTable=$SESSIONS_TABLE \
--parsingErrorsDestinationTable=$ERRORS_TABLE"
```
