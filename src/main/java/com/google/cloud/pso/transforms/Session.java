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

package com.google.cloud.pso.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.pso.data.CustomDataTypes.RideEvent;
import com.google.cloud.pso.data.CustomDataTypes.RideSession;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
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
              "AddKeys", WithKeys.of(RideEvent::getRideId).withKeyType(TypeDescriptors.strings()));

      // Use a late trigger only if late data wait is not zero
      Trigger sessionTrigger;
      if (lateDataWaitSeconds() > 0) {
        sessionTrigger =
            AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane())
                .withLateFirings(
                    AfterProcessingTime.pastFirstElementInPane()
                        .plusDelayOf(Duration.standardSeconds(lateDataWaitSeconds())));
      } else {
        sessionTrigger =
            AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane());
      }

      PCollection<KV<String, RideEvent>> sessions =
          withKeys.apply(
              "Window",
              Window.<KV<String, RideEvent>>into(
                      Sessions.withGapDuration(Duration.standardSeconds(sessionGapSeconds())))
                  .triggering(sessionTrigger)
                  .accumulatingFiredPanes()
                  .withAllowedLateness(Duration.standardSeconds(lateDataWaitSeconds())));

      PCollection<KV<String, RideAccumulator>> accumulated =
          sessions.apply("CombineEvents", Combine.perKey(new SessionPropertiesCombinerFn()));

      return accumulated.apply("SessionProperties", ParDo.of(new SessionPropertiesDoFn()));
    }
  }

  private static class SessionPropertiesCombinerFn
      extends CombineFn<RideEvent, RideAccumulator, RideAccumulator> {
    @Override
    public RideAccumulator createAccumulator() {
      return new RideAccumulator();
    }

    @Override
    public RideAccumulator addInput(RideAccumulator accumulator, RideEvent input) {
      Instant sessionBeginTimestamp = accumulator.beginTimestamp;
      Instant sessionEndTimestamp = accumulator.endTimestamp;

      if (sessionBeginTimestamp == null || input.getTimestamp().isBefore(sessionBeginTimestamp)) {
        accumulator.beginTimestamp = input.getTimestamp();
        accumulator.beginStatus = input.getRideStatus();
      }
      if (sessionEndTimestamp == null || input.getTimestamp().isAfter(sessionEndTimestamp)) {
        accumulator.endTimestamp = input.getTimestamp();
        accumulator.endStatus = input.getRideStatus();
      }

      accumulator.countEvents += 1;

      return accumulator;
    }

    @Override
    public RideAccumulator mergeAccumulators(Iterable<RideAccumulator> accumulators) {
      Instant begin = null;
      Instant end = null;
      String beginStatus = "";
      String endStatus = "";
      int count = 0;

      for (RideAccumulator accumulator : accumulators) {
        count += accumulator.countEvents;
        if (begin == null || accumulator.beginTimestamp.isBefore(begin)) {
          begin = accumulator.beginTimestamp;
          beginStatus = accumulator.beginStatus;
        }
        if (end == null || accumulator.endTimestamp.isAfter(end)) {
          end = accumulator.endTimestamp;
          endStatus = accumulator.endStatus;
        }
      }

      RideAccumulator merged = new RideAccumulator();
      merged.beginTimestamp = begin;
      merged.endTimestamp = end;
      merged.beginStatus = beginStatus;
      merged.endStatus = endStatus;
      merged.countEvents = count;

      return merged;
    }

    @Override
    public RideAccumulator extractOutput(RideAccumulator accumulator) {
      return accumulator;
    }
  }

  private static class SessionPropertiesDoFn
      extends DoFn<KV<String, RideAccumulator>, RideSession> {

    // For the full list of processElement parameters, see:
    // https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.ProcessElement.html
    @ProcessElement
    public void processElement(
        @Element KV<String, RideAccumulator> element,
        BoundedWindow window,
        PaneInfo pane,
        OutputReceiver<RideSession> receiver) {
      String sessionId = element.getKey();
      RideAccumulator accumulator = element.getValue();
      String windowId = window.toString();
      String paneInfo = pane.getTiming().toString();

      RideSession session =
          RideSession.builder()
              .setSessionId(sessionId)
              .setSessionBeginTimestamp(accumulator.beginTimestamp)
              .setSessionEndTimestamp(accumulator.endTimestamp)
              .setCountEvents(accumulator.countEvents)
              .setBeginStatus(accumulator.beginStatus)
              .setEndStatus(accumulator.endStatus)
              .setWindowId(windowId)
              .setTriggerInfo(paneInfo)
              .build();

      receiver.output(session);
    }
  }

  @DefaultCoder(AvroCoder.class)
  public static class RideAccumulator {
    public Integer countEvents = 0;

    public Instant beginTimestamp = null;

    public Instant endTimestamp = null;

    public String beginStatus = "";

    public String endStatus = "";

    public RideAccumulator() {}
  }

  private Session() {}
}
