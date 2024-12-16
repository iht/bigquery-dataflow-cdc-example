package com.google.cloud.pso.data;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.joda.time.Instant;

/** This class contains all the data types used in the pipeline. */
public final class CustomDataTypes {
  /** A ride event. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class RideEvent {
    @SchemaFieldName("ride_id")
    public abstract String getRideId();

    @SchemaFieldName("point_idx")
    public abstract Integer getPointIdx();

    @SchemaFieldName("latitude")
    public abstract Double getLatitude();

    @SchemaFieldName("longitude")
    public abstract Double getLongitude();

    @SchemaFieldName("timestamp")
    public abstract Instant getTimestamp();

    @SchemaFieldName("meter_reading")
    public abstract Double getMeterReading();

    @SchemaFieldName("meter_increment")
    public abstract Double getMeterIncrement();

    @SchemaFieldName("ride_status")
    public abstract String getRideStatus();

    @SchemaFieldName("passenger_count")
    public abstract Integer getPassengerCount();

    public static Builder builder() {
      return new AutoValue_CustomDataTypes_RideEvent.Builder();
    }

    /** Builder for {@link RideEvent}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setRideId(String rideId);

      public abstract Builder setPointIdx(Integer pointIdx);

      public abstract Builder setLatitude(Double latitude);

      public abstract Builder setLongitude(Double longitude);

      public abstract Builder setTimestamp(Instant timestamp);

      public abstract Builder setMeterReading(Double meterReading);

      public abstract Builder setMeterIncrement(Double meterIncrement);

      public abstract Builder setRideStatus(String rideStatus);

      public abstract Builder setPassengerCount(Integer passengerCount);

      public abstract RideEvent build();
    }
  }

  /** An aggregated session data */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class RideSession {
    @SchemaFieldName("session_id")
    public abstract String getSessionId();

    @SchemaFieldName("begin_timestamp")
    public abstract Instant getSessionBeginTimestamp();

    @SchemaFieldName("end_timestamp")
    public abstract Instant getSessionEndTimestamp();

    @SchemaFieldName("count_events")
    public abstract Integer getCountEvents();

    @SchemaFieldName("begin_status")
    public abstract String getBeginStatus();

    @SchemaFieldName("end_status")
    public abstract String getEndStatus();

    @SchemaFieldName("window_id")
    public abstract String getWindowId();

    @SchemaFieldName("trigger_info")
    public abstract String getTriggerInfo();

    public abstract RideSession.Builder toBuilder();

    public static Builder builder() {
      return new AutoValue_CustomDataTypes_RideSession.Builder();
    }

    /** Builder for {@link AggregatedSession}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setSessionId(String sessionId);

      public abstract Builder setSessionBeginTimestamp(Instant beginTimestamp);

      public abstract Builder setSessionEndTimestamp(Instant endTimestamp);

      public abstract Builder setCountEvents(Integer countEvents);

      public abstract Builder setBeginStatus(String beginStatus);

      public abstract Builder setEndStatus(String endStatus);

      public abstract Builder setWindowId(String windowId);

      public abstract Builder setTriggerInfo(String triggerInfo);

      public abstract RideSession build();
    }
  }

  /** A JSON parsing error data type. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class ParsingError {
    @SchemaFieldName("inputData")
    public abstract String getInputData();

    @SchemaFieldName("errorMessage")
    public abstract String getErrorMessage();

    @SchemaFieldName("timestamp")
    public abstract Instant getTimestamp();

    public static Builder builder() {
      return new AutoValue_CustomDataTypes_ParsingError.Builder();
    }

    /** Builder for {@link Error}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setInputData(String inputData);

      public abstract Builder setErrorMessage(String errorMessage);

      public abstract Builder setTimestamp(Instant timestamp);

      public abstract ParsingError build();
    }
  }

  private CustomDataTypes() {}
}
