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

package com.google.cloud.pso;

import com.google.cloud.pso.data.CustomDataTypes.RideSession;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import org.joda.time.Instant;

/** Common data used for several tests */
public final class TestData {

  public static ImmutableList<String> getKeys() {
    String key1 = "1";
    String key2 = "1";
    String key3 = "2";

    return ImmutableList.of(key1, key2, key3);
  }

  public static ImmutableList<String> getTimestampStrings() {
    String ts1 = "2019-09-09T11:42:48.33739-04:00";
    String ts2 = "2019-09-09T11:43:08.33739-04:00";
    String ts3 = "2019-09-09T11:59:48.33739-04:00";

    return ImmutableList.of(ts1, ts2, ts3);
  }

  public static List<Instant> getTimestamps() {
    return getTimestampStrings().stream().map(s -> Instant.parse(s)).collect(Collectors.toList());
  }

  public static ImmutableList<RideSession> getTestSessions() {
    RideSession s1 =
        RideSession.builder()
            .setSessionId(getKeys().get(0))
            .setSessionBeginTimestamp(getTimestamps().get(0))
            .setSessionEndTimestamp(getTimestamps().get(0))
            .setCountEvents(1)
            .setBeginStatus("pickup")
            .setEndStatus("pickup")
            .setTriggerInfo("EARLY")
            .setWindowId("[2019-09-09T15:42:48.337Z..2019-09-09T15:43:48.337Z)")
            .build();

    RideSession s2 =
        RideSession.builder()
            .setSessionId(getKeys().get(0))
            .setSessionBeginTimestamp(getTimestamps().get(0))
            .setSessionEndTimestamp(getTimestamps().get(1))
            .setCountEvents(2)
            .setBeginStatus("pickup")
            .setEndStatus("dropoff")
            .setTriggerInfo("EARLY")
            .setWindowId("[2019-09-09T15:42:48.337Z..2019-09-09T15:44:08.337Z)")
            .build();

    RideSession s3 =
        RideSession.builder()
            .setSessionId(getKeys().get(0))
            .setSessionBeginTimestamp(getTimestamps().get(0))
            .setSessionEndTimestamp(getTimestamps().get(1))
            .setCountEvents(2)
            .setBeginStatus("pickup")
            .setEndStatus("dropoff")
            .setTriggerInfo("ON_TIME")
            .setWindowId("[2019-09-09T15:42:48.337Z..2019-09-09T15:44:08.337Z)")
            .build();

    RideSession s4 =
        RideSession.builder()
            .setSessionId(getKeys().get(2))
            .setSessionBeginTimestamp(getTimestamps().get(2))
            .setSessionEndTimestamp(getTimestamps().get(2))
            .setCountEvents(1)
            .setBeginStatus("dropoff")
            .setEndStatus("dropoff")
            .setTriggerInfo("EARLY")
            .setWindowId("[2019-09-09T15:59:48.337Z..2019-09-09T16:00:48.337Z)")
            .build();

    RideSession s5 =
        RideSession.builder()
            .setSessionId(getKeys().get(2))
            .setSessionBeginTimestamp(getTimestamps().get(2))
            .setSessionEndTimestamp(getTimestamps().get(2))
            .setCountEvents(1)
            .setBeginStatus("dropoff")
            .setEndStatus("dropoff")
            .setTriggerInfo("ON_TIME")
            .setWindowId("[2019-09-09T15:59:48.337Z..2019-09-09T16:00:48.337Z)")
            .build();

    // Some events are repeated because of the accumualated panes mode
    return ImmutableList.of(s1, s2, s3, s4, s5);
  }

  public static ImmutableList<String> getTestData() {
    ImmutableList<String> keys = getKeys();

    String msg1 =
        String.format("{\"ride_id\": \"%s\",", keys.get(0))
            + "\"point_idx\": 1,"
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
        String.format("{\"ride_id\": \"%s\",", keys.get(1))
            + "\"point_idx\": 1,"
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
        String.format("{\"ride_id\": \"%s\",", keys.get(2))
            + "\"point_idx\": 2,"
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
