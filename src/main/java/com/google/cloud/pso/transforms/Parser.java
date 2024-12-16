package com.google.cloud.pso.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.pso.data.CustomDataTypes.ParsingError;
import com.google.cloud.pso.data.CustomDataTypes.RideEvent;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/** Parse JSON strings and return {@link Taxi.RideEvent} elements. */
public final class Parser {
  /** The main transform class. This is the public class exposed to users. */
  @AutoValue
  public abstract static class TaxiEventParser
      extends PTransform<PCollection<String>, PCollectionTuple> {

    public static final TupleTag<RideEvent> TAXI_EVENT_TAG = new TupleTag<>("TAXI_EVENT_TAG");

    public static final TupleTag<ParsingError> ERROR_TAG = new TupleTag<>("ERROR_TAG");

    public static TaxiEventParser parseJson() {
      return new AutoValue_Parser_TaxiEventParser();
    }

    @Override
    public PCollectionTuple expand(PCollection<String> input) {
      // Get rows
      PCollectionRowTuple allRows = input.apply("Json2Row", new Json2Row());
      PCollection<Row> goodRows = allRows.get(Json2Row.RESULTS_TAG);
      PCollection<Row> badRows = allRows.get(Json2Row.ERROR_TAG);

      // Convert rows to data classes
      PCollection<RideEvent> rideEvents =
          goodRows.apply("Row2TaxiRideEvent", Convert.fromRows(RideEvent.class));
      PCollection<ParsingError> errors = badRows.apply("Row2Error", new Row2ErrorMessage());

      return PCollectionTuple.of(TAXI_EVENT_TAG, rideEvents).and(ERROR_TAG, errors);
    }
  }

  /** Parses to Row and make sure the data conforms to the assumed schema. */
  private static class Json2Row extends PTransform<PCollection<String>, PCollectionRowTuple> {
    public static String RESULTS_TAG = "RESULTS_TAG";
    public static String ERROR_TAG = "ERROR_TAG";

    @Override
    public PCollectionRowTuple expand(PCollection<String> input) {
      Schema taxiRideSchema;

      try {
        taxiRideSchema = input.getPipeline().getSchemaRegistry().getSchema(RideEvent.class);
      } catch (NoSuchSchemaException e) {
        throw new IllegalStateException(
            String.format("No schema found for Taxi.RideEvent class: %s", e.getMessage()));
      }

      JsonToRow.ParseResult parseResult =
          input.apply(
              "Json2Row", JsonToRow.withExceptionReporting(taxiRideSchema).withExtendedErrorInfo());

      PCollection<Row> results = parseResult.getResults();
      PCollection<Row> errors = parseResult.getFailedToParseLines();

      return PCollectionRowTuple.of(RESULTS_TAG, results).and(ERROR_TAG, errors);
    }
  }

  private static class Row2ErrorMessage
      extends PTransform<PCollection<Row>, PCollection<ParsingError>> {
    @Override
    public PCollection<ParsingError> expand(PCollection<Row> input) {
      Schema errorSchema;
      try {
        errorSchema = input.getPipeline().getSchemaRegistry().getSchema(ParsingError.class);
      } catch (NoSuchSchemaException e) {
        throw new IllegalStateException(
            String.format("No schema found for Error class: %s", e.getMessage()));
      }

      PCollection<Row> rowsWithRightSchema =
          input.apply(
              "JsonRow2ErrorMessage", ParDo.of(new JsonRow2ErrorMessageRowDoFn(errorSchema)));

      return rowsWithRightSchema
          .setRowSchema(errorSchema)
          .apply("Row2ErrorMessage", Convert.fromRows(ParsingError.class));
    }
  }

  private static class JsonRow2ErrorMessageRowDoFn extends DoFn<Row, Row> {

    private final Schema errorRowSchema;

    JsonRow2ErrorMessageRowDoFn(Schema errorRowSchema) {
      this.errorRowSchema = errorRowSchema;
    }

    @ProcessElement
    public void processElement(
        @FieldAccess("line") String inputData,
        @FieldAccess("err") String errorMessage,
        @Timestamp Instant timestamp,
        OutputReceiver<Row> outputReceiver) {
      Row outputRow =
          Row.withSchema(this.errorRowSchema)
              .withFieldValue("inputData", inputData)
              .withFieldValue("errorMessage", errorMessage)
              .withFieldValue("timestamp", timestamp)
              .build();

      outputReceiver.output(outputRow);
    }
  }

  private Parser() {}
}
