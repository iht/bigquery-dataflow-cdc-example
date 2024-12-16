package com.google.cloud.pso.pipelines;

import com.google.api.services.bigquery.model.TableReference;
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
    TableReference rideEventsTable = new TableReference();
    rideEventsTable.setProjectId(opts.getProject());
    rideEventsTable.setDatasetId(opts.getDestinationDataset());
    rideEventsTable.setTableId(opts.getRidesDestinationTable());

    TableReference sessionsTable = new TableReference();
    sessionsTable.setProjectId(opts.getProject());
    sessionsTable.setDatasetId(opts.getDestinationDataset());
    sessionsTable.setTableId(opts.getSessionsDestinationTable());

    TableReference errorsTable = new TableReference();
    errorsTable.setProjectId(opts.getProject());
    errorsTable.setDatasetId(opts.getDestinationDataset());
    errorsTable.setTableId(opts.getParsingErrorsDestinationTable());

    Pipeline pipeline = Pipeline.create(opts);
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
    rideSessions.apply(
        "Upsert sessions in BQ",
        BigQueryIO.<RideSession>write()
            .to(
                (ValueInSingleWindow<RideSession> s) -> {
                  String table = s.getValue().getSessionId().substring(0, 2);
                  return new TableDestination(
                      String.format("%s:%s.%s", project, dataset, table),
                      "Just a table to showcase dynamic destinations with upserts");
                })
            .useBeamSchema()
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withPrimaryKey(ImmutableList.of("session_id"))
            .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
            .withRowMutationInformationFn(
                rs ->
                    RowMutationInformation.of(
                        RowMutationInformation.MutationType.UPSERT,
                        Integer.toHexString(rs.getCountEvents()))));

    return pipeline;
  }
}
