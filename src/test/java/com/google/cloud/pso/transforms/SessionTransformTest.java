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

import com.google.cloud.pso.TestData;
import com.google.cloud.pso.data.CustomDataTypes.RideEvent;
import com.google.cloud.pso.data.CustomDataTypes.RideSession;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class SessionTransformTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private PCollection<RideSession> sessions;
  private PCollection<Integer> sessionCount;

  @Before
  public void setUp() {

    ImmutableList<String> testData = TestData.getTestData();
    List<Instant> testTimestamps = TestData.getTimestamps();

    TestStream<String> testStream =
        TestStream.create(AvroCoder.of(String.class))
            .addElements(TimestampedValue.of(testData.get(0), testTimestamps.get(0)))
            .advanceProcessingTime(Duration.standardSeconds(1))
            .advanceWatermarkTo(testTimestamps.get(0))
            .addElements(TimestampedValue.of(testData.get(1), testTimestamps.get(1)))
            .advanceProcessingTime(Duration.standardSeconds(1))
            .advanceWatermarkTo(testTimestamps.get(1))
            .addElements(TimestampedValue.of(testData.get(2), testTimestamps.get(2)))
            .advanceProcessingTime(Duration.standardSeconds(1))
            .advanceWatermarkTo(testTimestamps.get(2))
            .advanceWatermarkToInfinity();

    PCollection<String> msgs = pipeline.apply("Create msgs", testStream);
    PCollectionTuple events = msgs.apply("Parse", Parser.TaxiEventParser.parseJson());
    PCollection<RideEvent> goodEvents = events.get(Parser.TaxiEventParser.TAXI_EVENT_TAG);

    sessions =
        goodEvents.apply(
            "CreateSession",
            Session.Calculate.builder().sessionGapSeconds(60).lateDataWaitSeconds(0).build());

    sessionCount =
        sessions.apply(
            "Get counts",
            MapElements.into(TypeDescriptors.integers()).via(s -> s.getCountEvents()));
  }

  @Test
  public void testSessionCreation() {
    PAssert.that(sessions).containsInAnyOrder(TestData.getTestSessions());
  }

  @Test
  public void testNumberOfEventsCalculation() {
    // Triggers are
    // key=1, count=1, EARLY
    // key=1, count=1,2, EARLY (accumulated)
    // key=1, count=2, ON_TIME
    // key=2, count=1, ON_TIME
    PAssert.that(sessionCount).containsInAnyOrder(ImmutableList.of(1, 1, 2, 2, 1));
  }

  @After
  public void tearDown() {
    pipeline.run().waitUntilFinish();
  }
}
