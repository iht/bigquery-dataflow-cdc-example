package com.google.cloud.pso.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;

/** All the options for the TaxiSessionPipeline */
public interface TaxiSessionsOptions extends GcpOptions {
  @Description("Pipeline to run")
  @Required
  String getPipeline();

  void setPipeline(String pipeline);

  @Description("PubSub Subscription to read ride events")
  @Required
  String getRideEventsSubscription();

  void setRideEventsSubscription(String subscription);

  @Description("BigQuery destination dataset")
  @Required
  String getDestinationDataset();

  void setDestinationDataset(String dataset);

  @Description("Destination table for the sessions")
  @Required
  String getSessionsDestinationTable();

  void setSessionsDestinationTable(String table);

  @Description("Destination table for the rides")
  @Required
  String getRidesDestinationTable();

  void setRidesDestinationTable(String table);

  @Description("Destination table for parsing errors")
  @Required
  String getParsingErrorsDestinationTable();

  void setParsingErrorsDestinationTable(String table);

  @Description("Session gap duration")
  @Default.Integer(30)
  Integer getSessionGapSeconds();

  void setSessionGapSeconds(Integer seconds);

  @Description("Late data wait time")
  @Default.Integer(10)
  Integer getLateDataWaitSeconds();

  void setLateDataWaitSeconds(Integer seconds);
}
