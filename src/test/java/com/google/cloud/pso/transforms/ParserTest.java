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
import com.google.cloud.pso.data.CustomDataTypes.ParsingError;
import com.google.cloud.pso.data.CustomDataTypes.RideEvent;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ParserTest {

  // Collections to be used for testing
  private PCollection<Long> errorsCount;
  private PCollection<Long> valuesCount;
  private PCollection<Integer> minutes;
  private PCollection<String> rideId;
  private PCollection<String> rideStatus;

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Before
  public void setUp() {

    PCollection<String> msgs =
        pipeline.apply(
            "Add data",
            Create.of(
                Iterables.concat(TestData.getTestData(), TestData.getMalFormattedTestData())));
    PCollectionTuple events = msgs.apply("Parse", Parser.TaxiEventParser.parseJson());
    PCollection<RideEvent> goodEvents = events.get(Parser.TaxiEventParser.TAXI_EVENT_TAG);
    PCollection<ParsingError> errors = events.get(Parser.TaxiEventParser.ERROR_TAG);

    errorsCount = errors.apply("Count errors", Count.globally());
    valuesCount = goodEvents.apply("Count values", Count.globally());

    minutes =
        goodEvents.apply(
            "Get minutes",
            MapElements.into(TypeDescriptors.integers())
                .via((RideEvent e) -> e.getTimestamp().toDateTime().getMinuteOfHour()));

    rideId =
        goodEvents.apply(
            "Get ride id",
            MapElements.into(TypeDescriptors.strings()).via((RideEvent e) -> e.getRideId()));

    rideStatus =
        goodEvents.apply(
            "Get ride status",
            MapElements.into(TypeDescriptors.strings()).via((RideEvent e) -> e.getRideStatus()));
  }

  @Test
  public void testJSONParsing() {
    PAssert.that(errorsCount).containsInAnyOrder(ImmutableList.of(1L));
    PAssert.that(valuesCount).containsInAnyOrder(ImmutableList.of(3L));
  }

  @Test
  public void testFieldsParsing() {
    PAssert.that(rideId).containsInAnyOrder(TestData.getKeys());
    PAssert.that(rideStatus).containsInAnyOrder(ImmutableList.of("pickup", "dropoff", "dropoff"));
  }

  @Test
  public void testTimestampsParsing() {
    PAssert.that(minutes).containsInAnyOrder(ImmutableList.of(42, 43, 59));
  }

  @After
  public void tearDown() {
    pipeline.run().waitUntilFinish();
  }
}
