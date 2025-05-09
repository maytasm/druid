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

package org.apache.druid.tests.indexer;

import com.google.common.collect.FluentIterable;
import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.druid.indexer.partitions.SecondaryPartitionType;
import org.apache.druid.indexer.report.IngestionStatsAndErrors;
import org.apache.druid.indexer.report.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexer.report.TaskContextReport;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.common.task.batch.parallel.PartialDimensionCardinalityTask;
import org.apache.druid.indexing.common.task.batch.parallel.PartialDimensionDistributionTask;
import org.apache.druid.indexing.common.task.batch.parallel.PartialGenericSegmentMergeTask;
import org.apache.druid.indexing.common.task.batch.parallel.PartialHashSegmentGenerateTask;
import org.apache.druid.indexing.common.task.batch.parallel.PartialRangeSegmentGenerateTask;
import org.apache.druid.indexing.common.task.batch.parallel.SinglePhaseSubTask;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.ClientInfoResourceTestClient;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.SqlTestQueryHelper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.testng.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public abstract class AbstractITBatchIndexTest extends AbstractIndexerTest
{
  public enum InputFormatDetails
  {
    AVRO("avro_ocf", ".avro", "/avro"),
    CSV("csv", ".csv", "/csv"),
    TSV("tsv", ".tsv", "/tsv"),
    ORC("orc", ".orc", "/orc"),
    JSON("json", ".json", "/json"),
    PARQUET("parquet", ".parquet", "/parquet");

    private final String inputFormatType;
    private final String fileExtension;
    private final String folderSuffix;

    InputFormatDetails(String inputFormatType, String fileExtension, String folderSuffix)
    {
      this.inputFormatType = inputFormatType;
      this.fileExtension = fileExtension;
      this.folderSuffix = folderSuffix;
    }

    public String getInputFormatType()
    {
      return inputFormatType;
    }

    public String getFileExtension()
    {
      return fileExtension;
    }

    public String getFolderSuffix()
    {
      return folderSuffix;
    }
  }

  public static final Logger LOG = new Logger(AbstractITBatchIndexTest.class);

  @Inject
  protected IntegrationTestingConfig config;
  @Inject
  protected SqlTestQueryHelper sqlQueryHelper;

  @Inject
  ClientInfoResourceTestClient clientInfoResourceTestClient;

  protected void doIndexTest(
      String dataSource,
      String indexTaskFilePath,
      String queryFilePath,
      boolean waitForNewVersion,
      boolean runTestQueries,
      boolean waitForSegmentsToLoad,
      Pair<Boolean, Boolean> segmentAvailabilityConfirmationPair
  ) throws IOException
  {
    doIndexTest(
        dataSource,
        indexTaskFilePath,
        Function.identity(),
        queryFilePath,
        waitForNewVersion,
        runTestQueries,
        waitForSegmentsToLoad,
        segmentAvailabilityConfirmationPair
    );
  }

  protected void doIndexTest(
      String dataSource,
      String indexTaskFilePath,
      Function<String, String> taskSpecTransform,
      String queryFilePath,
      boolean waitForNewVersion,
      boolean runTestQueries,
      boolean waitForSegmentsToLoad,
      Pair<Boolean, Boolean> segmentAvailabilityConfirmationPair
  ) throws IOException
  {
    doIndexTest(
        dataSource,
        indexTaskFilePath,
        taskSpecTransform,
        queryFilePath,
        Function.identity(),
        waitForNewVersion,
        runTestQueries,
        waitForSegmentsToLoad,
        segmentAvailabilityConfirmationPair
    );
  }

  protected void doIndexTest(
      String dataSource,
      String indexTaskFilePath,
      Function<String, String> taskSpecTransform,
      String queryFilePath,
      Function<String, String> queryTransform,
      boolean waitForNewVersion,
      boolean runTestQueries,
      boolean waitForSegmentsToLoad,
      Pair<Boolean, Boolean> segmentAvailabilityConfirmationPair
  ) throws IOException
  {
    final String fullDatasourceName = dataSource + config.getExtraDatasourceNameSuffix();
    final String taskSpec = taskSpecTransform.apply(
        StringUtils.replace(
            getResourceAsString(indexTaskFilePath),
            "%%DATASOURCE%%",
            fullDatasourceName
        )
    );

    submitTaskAndWait(
        taskSpec,
        fullDatasourceName,
        waitForNewVersion,
        waitForSegmentsToLoad,
        segmentAvailabilityConfirmationPair
    );
    if (runTestQueries) {
      doTestQuery(dataSource, queryFilePath, queryTransform);
    }
  }

  protected void doTestQuery(String dataSource, String queryFilePath)
  {
    doTestQuery(dataSource, queryFilePath, Function.identity());
  }

  protected void doTestQuery(String dataSource, String queryFilePath, Function<String, String> queryTransform)
  {
    try {
      String queryResponseTemplate;
      try {
        InputStream is = AbstractITBatchIndexTest.class.getResourceAsStream(queryFilePath);
        queryResponseTemplate = IOUtils.toString(is, StandardCharsets.UTF_8);
      }
      catch (IOException e) {
        throw new ISE(e, "could not read query file: %s", queryFilePath);
      }
      queryResponseTemplate = queryTransform.apply(
          StringUtils.replace(
              queryResponseTemplate,
              "%%DATASOURCE%%",
              dataSource + config.getExtraDatasourceNameSuffix()
          )
      );
      queryHelper.testQueriesFromString(queryResponseTemplate);
    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw new RuntimeException(e);
    }
  }

  protected void doReindexTest(
      String baseDataSource,
      String reindexDataSource,
      String reindexTaskFilePath,
      String queryFilePath,
      Pair<Boolean, Boolean> segmentAvailabilityConfirmationPair
  ) throws IOException
  {
    doReindexTest(
        baseDataSource,
        reindexDataSource,
        Function.identity(),
        reindexTaskFilePath,
        queryFilePath,
        segmentAvailabilityConfirmationPair
    );
  }

  void doReindexTest(
      String baseDataSource,
      String reindexDataSource,
      Function<String, String> taskSpecTransform,
      String reindexTaskFilePath,
      String queryFilePath,
      Pair<Boolean, Boolean> segmentAvailabilityConfirmationPair
  ) throws IOException
  {
    final String fullBaseDatasourceName = baseDataSource + config.getExtraDatasourceNameSuffix();
    final String fullReindexDatasourceName = reindexDataSource + config.getExtraDatasourceNameSuffix();

    String taskSpec = StringUtils.replace(
        getResourceAsString(reindexTaskFilePath),
        "%%DATASOURCE%%",
        fullBaseDatasourceName
    );

    taskSpec = StringUtils.replace(
        taskSpec,
        "%%REINDEX_DATASOURCE%%",
        fullReindexDatasourceName
    );

    taskSpec = taskSpecTransform.apply(taskSpec);

    submitTaskAndWait(
        taskSpec,
        fullReindexDatasourceName,
        false,
        true,
        segmentAvailabilityConfirmationPair
    );
    try {
      String queryResponseTemplate;
      try {
        InputStream is = AbstractITBatchIndexTest.class.getResourceAsStream(queryFilePath);
        queryResponseTemplate = IOUtils.toString(is, StandardCharsets.UTF_8);
      }
      catch (IOException e) {
        throw new ISE(e, "could not read query file: %s", queryFilePath);
      }

      queryResponseTemplate = StringUtils.replace(
          queryResponseTemplate,
          "%%DATASOURCE%%",
          fullReindexDatasourceName
      );

      queryHelper.testQueriesFromString(queryResponseTemplate);
      // verify excluded dimension is not reIndexed
      final List<String> dimensions = clientInfoResourceTestClient.getDimensions(
          fullReindexDatasourceName,
          "2013-08-31T00:00:00.000Z/2013-09-10T00:00:00.000Z"
      );
      Assert.assertFalse(dimensions.contains("robot"), "dimensions : " + dimensions);
    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw new RuntimeException(e);
    }
  }

  void doIndexTestSqlTest(
      String dataSource,
      String indexTaskFilePath,
      String queryFilePath
  ) throws IOException
  {
    doIndexTestSqlTest(
        dataSource,
        indexTaskFilePath,
        queryFilePath,
        Function.identity()
    );
  }
  void doIndexTestSqlTest(
      String dataSource,
      String indexTaskFilePath,
      String queryFilePath,
      Function<String, String> taskSpecTransform
  ) throws IOException
  {
    final String fullDatasourceName = dataSource + config.getExtraDatasourceNameSuffix();
    final String taskSpec = taskSpecTransform.apply(
        StringUtils.replace(
            getResourceAsString(indexTaskFilePath),
            "%%DATASOURCE%%",
            fullDatasourceName
        )
    );

    Pair<Boolean, Boolean> dummyPair = new Pair<>(false, false);
    submitTaskAndWait(taskSpec, fullDatasourceName, false, true, dummyPair);
    try {
      sqlQueryHelper.testQueriesFromFile(queryFilePath);
    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw new RuntimeException(e);
    }
  }

  protected void submitTaskAndWait(
      String taskSpec,
      String dataSourceName,
      boolean waitForNewVersion,
      boolean waitForSegmentsToLoad,
      Pair<Boolean, Boolean> segmentAvailabilityConfirmationPair
  )
  {
    // Wait for any existing kill tasks to complete before submitting new index task otherwise
    // kill tasks can fail with interval lock revoked.
    waitForAllTasksToCompleteForDataSource(dataSourceName);
    final List<DataSegment> oldVersions = waitForNewVersion ? coordinator.getAvailableSegments(dataSourceName) : null;

    long startSubTaskCount = -1;
    final boolean assertRunsSubTasks = taskSpec.contains("index_parallel");
    if (assertRunsSubTasks) {
      startSubTaskCount = countCompleteSubTasks(dataSourceName, !taskSpec.contains("dynamic"));
    }

    final String taskID = indexer.submitTask(taskSpec);
    LOG.info("Submitted task[%s]", taskID);
    indexer.waitUntilTaskCompletes(taskID);

    if (assertRunsSubTasks) {
      final boolean perfectRollup = !taskSpec.contains("dynamic");
      final long newSubTasks = countCompleteSubTasks(dataSourceName, perfectRollup) - startSubTaskCount;
      Assert.assertTrue(
          newSubTasks > 0,
          StringUtils.format(
              "The supervisor task[%s] didn't create any sub tasks. Was it executed in the parallel mode?",
              taskID
          )
      );
    }

    if (segmentAvailabilityConfirmationPair.lhs != null && segmentAvailabilityConfirmationPair.lhs) {
      TaskReport reportRaw = indexer.getTaskReport(taskID).get("ingestionStatsAndErrors");
      IngestionStatsAndErrorsTaskReport report = (IngestionStatsAndErrorsTaskReport) reportRaw;
      IngestionStatsAndErrors reportData = (IngestionStatsAndErrors) report.getPayload();

      // Confirm that the task waited longer than 0ms for the task to complete.
      Assert.assertTrue(reportData.getSegmentAvailabilityWaitTimeMs() > 0);

      // Make sure that the result of waiting for segments to load matches the expected result
      if (segmentAvailabilityConfirmationPair.rhs != null) {
        Assert.assertEquals(
            Boolean.valueOf(reportData.isSegmentAvailabilityConfirmed()),
            segmentAvailabilityConfirmationPair.rhs
        );
      }

      TaskContextReport taskContextReport =
          (TaskContextReport) indexer.getTaskReport(taskID).get(TaskContextReport.REPORT_KEY);

      Assert.assertFalse(taskContextReport.getPayload().isEmpty());
    }

    // IT*ParallelIndexTest do a second round of ingestion to replace segements in an existing
    // data source. For that second round we need to make sure the coordinator actually learned
    // about the new segments befor waiting for it to report that all segments are loaded; otherwise
    // this method could return too early because the coordinator is merely reporting that all the
    // original segments have loaded.
    if (waitForNewVersion) {
      ITRetryUtil.retryUntilTrue(
          () -> {
            final SegmentTimeline timeline = SegmentTimeline.forSegments(
                coordinator.getAvailableSegments(dataSourceName)
            );

            final List<TimelineObjectHolder<String, DataSegment>> holders = timeline.lookup(Intervals.ETERNITY);
            return FluentIterable
                .from(holders)
                .transformAndConcat(TimelineObjectHolder::getObject)
                .anyMatch(
                    chunk -> FluentIterable.from(oldVersions)
                                           .anyMatch(oldSegment -> chunk.getObject().overshadows(oldSegment))
                );
          },
          "See a new version"
      );
    }

    if (waitForSegmentsToLoad) {
      ITRetryUtil.retryUntilTrue(
          () -> coordinator.areSegmentsLoaded(dataSourceName), "Segment Load"
      );
    }
  }

  private long countCompleteSubTasks(final String dataSource, final boolean perfectRollup)
  {
    return indexer.getCompleteTasksForDataSource(dataSource)
                  .stream()
                  .filter(t -> {
                    if (!perfectRollup) {
                      return t.getType().equals(SinglePhaseSubTask.TYPE);
                    } else {
                      return t.getType().equalsIgnoreCase(PartialHashSegmentGenerateTask.TYPE)
                             || t.getType().equalsIgnoreCase(PartialDimensionDistributionTask.TYPE)
                             || t.getType().equalsIgnoreCase(PartialDimensionCardinalityTask.TYPE)
                             || t.getType().equalsIgnoreCase(PartialRangeSegmentGenerateTask.TYPE)
                             || t.getType().equalsIgnoreCase(PartialGenericSegmentMergeTask.TYPE);
                    }
                  })
                  .count();
  }

  void verifySegmentsCountAndLoaded(String dataSource, int numExpectedSegments)
  {
    ITRetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(dataSource + config.getExtraDatasourceNameSuffix()),
        "Segments are loaded"
    );
    ITRetryUtil.retryUntilEquals(
        () -> coordinator.getAvailableSegments(
            dataSource + config.getExtraDatasourceNameSuffix()
        ).size(),
        numExpectedSegments,
        "Segment count"
    );
  }

  void verifySegmentsCountAndLoaded(String dataSource, int numExpectedSegments, int numExpectedTombstones)
  {
    ITRetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(dataSource + config.getExtraDatasourceNameSuffix()),
        "Segments are loaded"
    );
    ITRetryUtil.retryUntilEquals(
        () -> coordinator.getAvailableSegments(dataSource + config.getExtraDatasourceNameSuffix()).size(),
        numExpectedSegments,
        "Segment count"
    );
    ITRetryUtil.retryUntilEquals(
        () -> (int) coordinator.getAvailableSegments(dataSource + config.getExtraDatasourceNameSuffix())
                               .stream().filter(DataSegment::isTombstone).count(),
        numExpectedTombstones,
        "Tombstone count"
    );
  }

  void compactData(String dataSource, String compactionTask) throws Exception
  {
    final String fullDatasourceName = dataSource + config.getExtraDatasourceNameSuffix();
    final List<String> intervalsBeforeCompaction = coordinator.getSegmentIntervals(fullDatasourceName);
    intervalsBeforeCompaction.sort(null);
    final String template = getResourceAsString(compactionTask);
    String taskSpec = StringUtils.replace(template, "%%DATASOURCE%%", fullDatasourceName);

    final String taskID = indexer.submitTask(taskSpec);
    LOG.info("Submitted task[%s] for compaction", taskID);
    indexer.waitUntilTaskCompletes(taskID);

    ITRetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(fullDatasourceName),
        "Segments are loaded after compaction"
    );
    ITRetryUtil.retryUntilEquals(
        () -> {
          final List<String> actualIntervals = coordinator.getSegmentIntervals(
              dataSource + config.getExtraDatasourceNameSuffix()
          );
          actualIntervals.sort(null);
          return actualIntervals;
        },
        intervalsBeforeCompaction,
        "Compacted intervals"
    );
  }

  void verifySegmentsCompacted(String dataSource, int expectedCompactedSegmentCount)
  {
    List<DataSegment> segments = coordinator.getFullSegmentsMetadata(
        dataSource + config.getExtraDatasourceNameSuffix()
    );
    List<DataSegment> foundCompactedSegments = new ArrayList<>();
    for (DataSegment segment : segments) {
      if (segment.getLastCompactionState() != null) {
        foundCompactedSegments.add(segment);
      }
    }
    Assert.assertEquals(foundCompactedSegments.size(), expectedCompactedSegmentCount);
    for (DataSegment compactedSegment : foundCompactedSegments) {
      Assert.assertNotNull(compactedSegment.getLastCompactionState());
      Assert.assertNotNull(compactedSegment.getLastCompactionState().getPartitionsSpec());
      Assert.assertEquals(
          compactedSegment.getLastCompactionState().getPartitionsSpec().getType(),
          SecondaryPartitionType.LINEAR
      );
    }
  }
}
