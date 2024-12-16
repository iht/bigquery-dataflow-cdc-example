package com.google.cloud.pso.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.pso.data.CustomDataTypes.RideEvent;
import com.google.cloud.pso.data.CustomDataTypes.RideSession;
import java.util.Optional;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Calculate sessions for a collection of {@link RideEvent}. */
public final class Session {

  /** The main transform calculating sessions. */
  @AutoValue
  public abstract static class Calculate
      extends PTransform<PCollection<RideEvent>, PCollection<RideSession>> {

    public abstract Integer sessionGapSeconds();

    public abstract Integer lateDataWaitSeconds();

    public static Builder builder() {
      return new AutoValue_Session_Calculate.Builder();
    }

    /** Builder for {@link SessionTransform.Calculate}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder sessionGapSeconds(Integer sessionGapSeconds);

      public abstract Builder lateDataWaitSeconds(Integer lateDataWaitSeconds);

      public abstract Calculate build();
    }

    @Override
    public PCollection<RideSession> expand(PCollection<RideEvent> rideEvents) {

      // Add keys to rideEvents
      PCollection<KV<String, RideEvent>> withKeys =
          rideEvents.apply(
              "Add keys", WithKeys.of(RideEvent::getRideId).withKeyType(TypeDescriptors.strings()));

      // Use a late trigger only if late data wait is not zero
      Trigger sessionTrigger;
      if (lateDataWaitSeconds() > 0) {
        sessionTrigger =
            AfterWatermark.pastEndOfWindow()
                .withLateFirings(
                    AfterProcessingTime.pastFirstElementInPane()
                        .plusDelayOf(Duration.standardSeconds(lateDataWaitSeconds())));
      } else {
        sessionTrigger = AfterWatermark.pastEndOfWindow();
      }

      PCollection<KV<String, RideEvent>> sessions =
          withKeys.apply(
              "Window",
              Window.<KV<String, RideEvent>>into(
                      Sessions.withGapDuration(Duration.standardSeconds(sessionGapSeconds())))
                  .triggering(sessionTrigger)
                  .accumulatingFiredPanes()
                  .withAllowedLateness(Duration.standardSeconds(lateDataWaitSeconds())));

      PCollection<KV<String, Iterable<RideEvent>>> grouped =
          sessions.apply("Group by ride id", GroupByKey.create());

      return grouped.apply("Session properties", ParDo.of(new SessionPropertiesDoFn()));
    }
  }

  private static class SessionPropertiesDoFn
      extends DoFn<KV<String, Iterable<RideEvent>>, RideSession> {

    // For the full list of processElement parameters, see:
    // https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.ProcessElement.html
    @ProcessElement
    public void processElement(
        @Element KV<String, Iterable<RideEvent>> element,
        BoundedWindow window,
        PaneInfo pane,
        OutputReceiver<RideSession> receiver) {
      // Traverse the iterable
      String sessionId = element.getKey();
      Iterable<RideEvent> events = element.getValue();
      String windowId = window.toString();
      String paneInfo = pane.getTiming().toString();
      RideSession session = populateSessionProperties(events, sessionId, windowId, paneInfo);
      receiver.output(session);
    }

    private RideSession populateSessionProperties(
        Iterable<RideEvent> events, String sessionId, String window, String paneInfo) {
      Optional<Instant> startTime = Optional.empty();
      Optional<Instant> endTime = Optional.empty();
      String beginStatus = "";
      String endStatus = "";
      int count = 0;
      for (RideEvent event : events) {
        Instant currentTime = event.getTimestamp();
        String status = event.getRideStatus();
        // Initialize min and max variables
        if (count == 0) {
          startTime = Optional.of(currentTime);
          beginStatus = status;

          endTime = Optional.of(currentTime);
          endStatus = status;
        }

        if (currentTime.isBefore(startTime.get())) {
          startTime = Optional.of(currentTime);
          beginStatus = status;
        }
        if (currentTime.isAfter(endTime.get())) {
          endTime = Optional.of(currentTime);
          endStatus = status;
        }

        count++;
      }

      return RideSession.builder()
          .setSessionId(sessionId)
          .setSessionBeginTimestamp(startTime.get())
          .setSessionEndTimestamp(endTime.get())
          .setCountEvents(count)
          .setBeginStatus(beginStatus)
          .setEndStatus(endStatus)
          .setWindowId(window)
          .setTriggerInfo(paneInfo)
          .build();
    }
  }

  private Session() {}
}
