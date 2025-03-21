/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.timeline;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

public class SegmentTimelineTest
{
  @Test
  public void testIsOvershadowed()
  {
    final SegmentTimeline timeline = SegmentTimeline.forSegments(
        Arrays.asList(
            createSegment("2022-01-01/2022-01-02", "v1", 0, 3),
            createSegment("2022-01-01/2022-01-02", "v1", 1, 3),
            createSegment("2022-01-01/2022-01-02", "v1", 2, 3),
            createSegment("2022-01-02/2022-01-03", "v2", 0, 2),
            createSegment("2022-01-02/2022-01-03", "v2", 1, 2)
        )
    );

    Assert.assertFalse(
        timeline.isOvershadowed(createSegment("2022-01-01/2022-01-02", "v1", 1, 3))
    );
    Assert.assertFalse(
        timeline.isOvershadowed(createSegment("2022-01-01/2022-01-02", "v1", 2, 3))
    );
    Assert.assertFalse(
        timeline.isOvershadowed(createSegment("2022-01-01/2022-01-02", "v1", 1, 4))
    );
    Assert.assertFalse(
        timeline.isOvershadowed(createSegment("2022-01-01T00:00:00/2022-01-01T06:00:00", "v1", 1, 4))
    );

    Assert.assertTrue(
        timeline.isOvershadowed(createSegment("2022-01-02/2022-01-03", "v1", 2, 4))
    );
    Assert.assertTrue(
        timeline.isOvershadowed(createSegment("2022-01-02/2022-01-03", "v1", 0, 1))
    );
  }

  @Test
  public void testAddRemoveSegment()
  {
    final DataSegment segment = createSegment("2022-01-01/P1D", "v1", 0, 1);

    final SegmentTimeline timeline = SegmentTimeline.forSegments(Set.of());
    timeline.add(segment);
    Assert.assertEquals(1, timeline.getNumObjects());

    timeline.remove(segment);
    Assert.assertEquals(0, timeline.getNumObjects());
    Assert.assertTrue(timeline.isEmpty());
  }

  private DataSegment createSegment(String interval, String version, int partitionNum, int totalNumPartitions)
  {
    return new DataSegment(
        TestDataSource.WIKI,
        Intervals.of(interval),
        version,
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        new NumberedShardSpec(partitionNum, totalNumPartitions),
        0x9,
        1L
    );
  }
}
