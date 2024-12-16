package com.google.cloud.pso;

import com.google.cloud.pso.data.CustomDataTypes.RideSession;
import com.google.common.collect.ImmutableList;
import org.joda.time.Instant;

/** Common data used for several tests */
public final class TestData {

  public static ImmutableList<String> getKeys() {
    String key1 = "0878437d-8dae-4ebc-b4cf-3a3da40396ad";
    String key2 = "1878437d-7dae-4ebc-b4cf-3a3da40396ad";
    String key3 = key1;

    return ImmutableList.of(key1, key2, key3);
  }

  public static ImmutableList<String> getTimestampStrings() {
    String ts1 = "2019-09-09T11:42:48.33739-04:00";
    String ts2 = "2019-09-09T11:52:48.33739-04:00";
    String ts3 = "2019-09-09T11:49:48.33739-04:00";

    return ImmutableList.of(ts1, ts2, ts3);
  }

  public static ImmutableList<Instant> getTimestamps() {
    Instant t1 = Instant.parse("2019-09-09T11:42:48.33739-04:00");
    Instant t2 = Instant.parse("2019-09-09T11:52:48.33739-04:00");
    Instant t3 = Instant.parse("2019-09-09T11:49:48.33739-04:00");

    return ImmutableList.of(t1, t2, t3);
  }

  public static ImmutableList<RideSession> getTestSessions() {
    RideSession s1 =
        RideSession.builder()
            .setSessionId(getKeys().get(0))
            .setSessionBeginTimestamp(getTimestamps().get(0))
            .setSessionEndTimestamp(getTimestamps().get(2))
            .setCountEvents(2)
            .setBeginStatus("pickup")
            .setEndStatus("dropoff")
            .setTriggerInfo("ON_TIME")
            .setWindowId("-290308-12-21T19:59:05.225Z..-290308-12-21T19:59:35.225Z)")
            .build();

    RideSession s2 =
        RideSession.builder()
            .setSessionId(getKeys().get(1))
            .setSessionBeginTimestamp(getTimestamps().get(1))
            .setSessionEndTimestamp(getTimestamps().get(1))
            .setCountEvents(1)
            .setBeginStatus("dropoff")
            .setEndStatus("dropoff")
            .setTriggerInfo("ON_TIME")
            .setWindowId("[-290308-12-21T19:59:05.225Z..-290308-12-21T19:59:35.225Z)")
            .build();

    return ImmutableList.of(s1, s2);
  }

  public static ImmutableList<String> getTestData() {
    ImmutableList<String> keys = getKeys();
    String key1 = keys.get(0);
    String key2 = keys.get(1);

    String msg1 =
        String.format("{\"ride_id\": \"%s\",", key1)
            + "\"point_idx\": 160,"
            + "\"latitude\": 40.780210000000004,"
            + "\"longitude\": -73.97139,"
            + "\"timestamp\": \""
            + getTimestampStrings().get(0)
            + "\","
            + "\"meter_reading\": 5.746939,"
            + "\"meter_increment\": 0.03591837,"
            + "\"ride_status\": \"pickup\","
            + "\"passenger_count\": 2}";

    String msg2 =
        String.format("{\"ride_id\": \"%s\",", key2)
            + "\"point_idx\": 161,"
            + "\"latitude\": 40.180210000000004,"
            + "\"longitude\": -73.17139,"
            + "\"timestamp\": \""
            + getTimestampStrings().get(1)
            + "\","
            + "\"meter_reading\": 6,"
            + "\"meter_increment\": 0.03591837,"
            + "\"ride_status\": \"dropoff\","
            + "\"passenger_count\": 2}";

    String msg3 =
        String.format("{\"ride_id\": \"%s\",", key1)
            + "\"point_idx\": 160,"
            + "\"latitude\": 40.780210000000004,"
            + "\"longitude\": -73.97139,"
            + "\"timestamp\": \""
            + getTimestampStrings().get(2)
            + "\","
            + "\"meter_reading\": 5.746939,"
            + "\"meter_increment\": 0.03591837,"
            + "\"ride_status\": \"dropoff\","
            + "\"passenger_count\": 2}";

    return ImmutableList.of(msg1, msg2, msg3);
  }

  public static ImmutableList<String> getMalFormattedTestData() {
    String malformed =
        "{\"ride_id\": \"malformed\","
            + "\"point_idx\": 161"
            + "\"latitude\": 40.180210000000004"
            + "\"longitude\": -73.17139,"
            + "\"timestamp\": \"2019-09-09T11:52:48.33739-04:00\","
            + "\"meter_reading\": 6"
            + "\"meter_increment\": 0.03591837,"
            + "\"ride_status\": dropoff\","
            + "\"passenger_count\": 2}";

    return ImmutableList.of(malformed);
  }

  private TestData() {}
}
