package com.google.cloud.pso.transforms;

import com.google.cloud.pso.TestData;
import com.google.cloud.pso.data.CustomDataTypes.RideEvent;
import com.google.cloud.pso.data.CustomDataTypes.RideSession;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
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
  private PCollection<Long> sessionCount;

  @Before
  public void setUp() {

    ImmutableList<String> testData = TestData.getTestData();
    ImmutableList<Instant> testTimestamps = TestData.getTimestamps();

    TestStream<String> testStream =
        TestStream.create(AvroCoder.of(String.class))
            .addElements(testData.get(0))
            .advanceProcessingTime(Duration.standardSeconds(2))
            .advanceWatermarkTo(testTimestamps.get(0))
            .addElements(testData.get(1))
            .advanceProcessingTime(Duration.standardSeconds(10))
            .advanceWatermarkTo(testTimestamps.get(1))
            .advanceWatermarkToInfinity();

    PCollection<String> msgs = pipeline.apply("Create msgs", testStream);
    PCollectionTuple events = msgs.apply("Parse", Parser.TaxiEventParser.parseJson());
    PCollection<RideEvent> goodEvents = events.get(Parser.TaxiEventParser.TAXI_EVENT_TAG);

    sessions =
        goodEvents.apply(
            "CreateSession",
            Session.Calculate.builder().sessionGapSeconds(30).lateDataWaitSeconds(10).build());

    sessionCount =
        sessions.apply("Count", Combine.globally(Count.<RideSession>combineFn()).withoutDefaults());
  }

  @Test
  public void testSessionCreation() {
    // Two different sessions
    // PAssert.that(sessions).containsInAnyOrder(TestData.getTestSessions());
    PAssert.that(sessionCount).containsInAnyOrder(ImmutableList.of(1L, 1L));
  }

  @After
  public void tearDown() {
    pipeline.run().waitUntilFinish();
  }
}
