/*
 *  Copyright 2024 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.google.cloud.pso.pipelines;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.pso.data.CustomDataTypes.ParsingError;
import com.google.cloud.pso.data.CustomDataTypes.RideEvent;
import com.google.cloud.pso.data.CustomDataTypes.RideSession;
import com.google.cloud.pso.options.TaxiSessionsOptions;
import com.google.cloud.pso.transforms.Parser;
import com.google.cloud.pso.transforms.Session;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Main pipeline class. */
public final class TaxiSessionsPipeline {

  public static final Logger LOG = LoggerFactory.getLogger(TaxiSessionsPipeline.class);

  public static Pipeline createPipeline(TaxiSessionsOptions opts) {
    // Destination tables
    TableReference sessionsTable = new TableReference();
    sessionsTable.setProjectId(opts.getProject());
    sessionsTable.setDatasetId(opts.getDestinationDataset());
    sessionsTable.setTableId(opts.getSessionsDestinationTable());

    TableReference errorsTable = new TableReference();
    errorsTable.setProjectId(opts.getProject());
    errorsTable.setDatasetId(opts.getDestinationDataset());
    errorsTable.setTableId(opts.getParsingErrorsDestinationTable());

    Pipeline pipeline = Pipeline.create(opts);
    // The messages in the Pubsub topic have an attribute (metadata) with their timestamp, let's
    // use that attribute to set the timestamp for our messages and work on event time (windows,
    // triggers, etc).
    PCollection<String> rides =
        pipeline.apply(
            "Read rides",
            PubsubIO.readStrings()
                .fromSubscription(opts.getRideEventsSubscription())
                .withTimestampAttribute("ts"));

    PCollectionTuple parsed = rides.apply("Parse JSON", Parser.TaxiEventParser.parseJson());
    PCollection<RideEvent> rideEvents = parsed.get(Parser.TaxiEventParser.TAXI_EVENT_TAG);
    PCollection<ParsingError> parsingErrors = parsed.get(Parser.TaxiEventParser.ERROR_TAG);

    PCollection<RideSession> rideSessions =
        rideEvents.apply(
            "Sessions",
            Session.Calculate.builder()
                .sessionGapSeconds(opts.getSessionGapSeconds())
                .lateDataWaitSeconds(opts.getLateDataWaitSeconds())
                .build());

    parsingErrors.apply(
        "Append errors to BQ",
        BigQueryIO.<ParsingError>write()
            .to(errorsTable)
            .useBeamSchema()
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE));

    // Upsert the session data
    String project = opts.getProject();
    String dataset = opts.getDestinationDataset();

    // Primary key for CDC and clustering for performance
    ImmutableList<String> primaryKeys = ImmutableList.of("session_id");
    Clustering clustering = new Clustering();
    clustering.setFields(primaryKeys);

    SerializableFunction<ValueInSingleWindow<RideSession>, TableDestination> tableFunc =
        (ValueInSingleWindow<RideSession> s) -> rideSessionTDestination(s, project, dataset);

    rideSessions.apply(
        "Upsert sessions in BQ",
        BigQueryIO.<RideSession>write()
            .to(tableFunc)
            .useBeamSchema()
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withPrimaryKey(primaryKeys)
            .withClustering(clustering)
            .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
            .withRowMutationInformationFn(
                rs ->
                    RowMutationInformation.of(
                        RowMutationInformation.MutationType.UPSERT,
                        Integer.toHexString(rs.getCountEvents()))));

    return pipeline;
  }

  private static TableDestination rideSessionTDestination(
      ValueInSingleWindow<RideSession> s, String project, String dataset) {
    // TimePartitioning is not serializable, so we need to wrap it in this function to be able to
    // pass it to BigQueryIO.

    // Partitioning by ingestion day
    TimePartitioning partitioning = new TimePartitioning();
    // By default, we partition by _INGESTIONTIME, but we could partition by other fields
    // for instance: partitioning.setField("end_timestamp");
    partitioning.setType("DAY");
    partitioning.setExpirationMs(
        365L * 24L * 3600L * 1000L); // 1 year of expiration time for the partitions

    String table = String.format("sesions_%s", s.getValue().getSessionId().substring(0, 1));
    return new TableDestination(
        String.format("%s:%s.%s", project, dataset, table),
        "Just a table to showcase dynamic destinations with upserts",
        partitioning);
  }
}
