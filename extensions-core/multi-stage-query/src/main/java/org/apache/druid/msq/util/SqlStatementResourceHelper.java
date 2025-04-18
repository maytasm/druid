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


package org.apache.druid.msq.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.NotFound;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.CounterSnapshots;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.counters.QueryCounterSnapshot;
import org.apache.druid.msq.counters.SegmentGenerationProgressCounter;
import org.apache.druid.msq.exec.ResultsContext;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.destination.DurableStorageMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.indexing.report.MSQStagesReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.sql.SqlStatementState;
import org.apache.druid.msq.sql.entity.ColumnNameAndTypes;
import org.apache.druid.msq.sql.entity.PageInformation;
import org.apache.druid.msq.sql.entity.SqlStatementResult;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.run.SqlResults;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqlStatementResourceHelper
{
  public static Optional<List<ColumnNameAndTypes>> getSignature(
      MSQControllerTask msqControllerTask
  )
  {
    // only populate signature for select q's
    if (!MSQControllerTask.isIngestion(msqControllerTask.getQuerySpec())) {
      ColumnMappings columnMappings = msqControllerTask.getQuerySpec().getColumnMappings();
      List<SqlTypeName> sqlTypeNames = msqControllerTask.getSqlTypeNames();
      if (sqlTypeNames == null || sqlTypeNames.size() != columnMappings.size()) {
        return Optional.empty();
      }
      List<ColumnType> nativeTypeNames = msqControllerTask.getNativeTypeNames();
      if (nativeTypeNames == null || nativeTypeNames.size() != columnMappings.size()) {
        return Optional.empty();
      }
      List<ColumnNameAndTypes> signature = new ArrayList<>(columnMappings.size());
      int index = 0;
      for (String colName : columnMappings.getOutputColumnNames()) {
        signature.add(new ColumnNameAndTypes(
            colName,
            sqlTypeNames.get(index).getName(),
            nativeTypeNames.get(index).asTypeString()
        ));
        index++;
      }
      return Optional.of(signature);
    }
    return Optional.empty();
  }


  public static void isMSQPayload(TaskPayloadResponse taskPayloadResponse, String queryId) throws DruidException
  {
    if (taskPayloadResponse == null || taskPayloadResponse.getPayload() == null) {
      throw NotFound.exception("Query[%s] not found", queryId);
    }

    if (MSQControllerTask.class != taskPayloadResponse.getPayload().getClass()) {
      throw NotFound.exception("Query[%s] not found", queryId);
    }
  }

  public static SqlStatementState getSqlStatementState(TaskStatusPlus taskStatusPlus)
  {
    TaskState state = taskStatusPlus.getStatusCode();
    if (state == null) {
      return SqlStatementState.ACCEPTED;
    }

    switch (state) {
      case FAILED:
        return SqlStatementState.FAILED;
      case RUNNING:
        if (TaskLocation.unknown().equals(taskStatusPlus.getLocation())) {
          return SqlStatementState.ACCEPTED;
        } else {
          return SqlStatementState.RUNNING;
        }
      case SUCCESS:
        return SqlStatementState.SUCCESS;
      default:
        throw new ISE("Unrecognized state[%s] found.", state);
    }
  }

  /**
   * Populates pages list from the {@link CounterSnapshotsTree}.
   * <br>
   * The number of pages changes with respect to the destination
   * <ol>
   *   <li>{@link DataSourceMSQDestination} a single page is returned which adds all the counters of {@link SegmentGenerationProgressCounter.Snapshot}</li>
   *   <li>{@link TaskReportMSQDestination} a single page is returned which adds all the counters of {@link ChannelCounters}</li>
   *   <li>{@link DurableStorageMSQDestination} a page is returned for each partition, worker which has generated output rows. The pages are populated in the following order:
   *   <ul>
   *     <li>For each partition from 0 to N</li>
   *     <li>For each worker from 0 to M</li>
   *     <li>If num rows for that partition,worker combination is 0, create a page</li>
   *     so that we maintain the record ordering.
   *   </ul>
   * </ol>
   */
  public static Optional<List<PageInformation>> populatePageList(
      MSQTaskReportPayload msqTaskReportPayload,
      MSQDestination msqDestination
  )
  {
    if (msqTaskReportPayload.getStages() == null || msqTaskReportPayload.getCounters() == null) {
      return Optional.empty();
    }
    MSQStagesReport.Stage finalStage = getFinalStage(msqTaskReportPayload);
    CounterSnapshotsTree counterSnapshotsTree = msqTaskReportPayload.getCounters();
    Map<Integer, CounterSnapshots> workerCounters = counterSnapshotsTree.snapshotForStage(finalStage.getStageNumber());
    if (workerCounters == null || workerCounters.isEmpty()) {
      return Optional.empty();
    }

    if (msqDestination instanceof DataSourceMSQDestination) {
      long rows = 0L;
      for (CounterSnapshots counterSnapshots : workerCounters.values()) {
        QueryCounterSnapshot queryCounterSnapshot = counterSnapshots.getMap()
                                                                    .getOrDefault("segmentGenerationProgress", null);
        if (queryCounterSnapshot instanceof SegmentGenerationProgressCounter.Snapshot) {
          rows += ((SegmentGenerationProgressCounter.Snapshot) queryCounterSnapshot).getRowsPushed();
        }
      }
      return Optional.of(ImmutableList.of(new PageInformation(0, rows, null)));
    } else if (msqDestination instanceof TaskReportMSQDestination) {
      long rows = 0L;
      long size = 0L;
      for (CounterSnapshots counterSnapshots : workerCounters.values()) {
        QueryCounterSnapshot queryCounterSnapshot = counterSnapshots.getMap().getOrDefault("output", null);
        if (queryCounterSnapshot instanceof ChannelCounters.Snapshot) {
          rows += Arrays.stream(((ChannelCounters.Snapshot) queryCounterSnapshot).getRows()).sum();
          size += Arrays.stream(((ChannelCounters.Snapshot) queryCounterSnapshot).getBytes()).sum();
        }
      }
      return Optional.of(ImmutableList.of(new PageInformation(0, rows, size)));
    } else if (msqDestination instanceof DurableStorageMSQDestination) {

      return populatePagesForDurableStorageDestination(finalStage, workerCounters);
    } else {
      return Optional.empty();
    }
  }

  private static Optional<List<PageInformation>> populatePagesForDurableStorageDestination(
      MSQStagesReport.Stage finalStage,
      Map<Integer, CounterSnapshots> workerCounters
  )
  {
    // figure out number of partitions and number of workers
    int totalPartitions = finalStage.getPartitionCount();
    int totalWorkerCount = finalStage.getWorkerCount();

    if (totalPartitions == -1) {
      throw DruidException.defensive("Expected partition count to be set for stage[%d]", finalStage);
    }
    if (totalWorkerCount == -1) {
      throw DruidException.defensive("Expected worker count to be set for stage[%d]", finalStage);
    }

    List<PageInformation> pages = new ArrayList<>();
    for (int partitionNumber = 0; partitionNumber < totalPartitions; partitionNumber++) {
      for (int workerNumber = 0; workerNumber < totalWorkerCount; workerNumber++) {
        CounterSnapshots workerCounter = workerCounters.get(workerNumber);

        if (workerCounter != null && workerCounter.getMap() != null) {
          QueryCounterSnapshot channelCounters = workerCounter.getMap().get("output");

          if (channelCounters instanceof ChannelCounters.Snapshot) {
            long rows = 0L;
            long size = 0L;

            if (((ChannelCounters.Snapshot) channelCounters).getRows().length > partitionNumber) {
              rows += ((ChannelCounters.Snapshot) channelCounters).getRows()[partitionNumber];
              size += ((ChannelCounters.Snapshot) channelCounters).getBytes()[partitionNumber];
            }
            if (rows != 0L) {
              pages.add(new PageInformation(pages.size(), rows, size, workerNumber, partitionNumber));
            }
          }
        }
      }
    }
    return Optional.of(pages);
  }

  public static Optional<SqlStatementResult> getExceptionPayload(
      String queryId,
      TaskStatusResponse taskResponse,
      TaskStatusPlus statusPlus,
      SqlStatementState sqlStatementState,
      MSQTaskReportPayload msqTaskReportPayload,
      ObjectMapper jsonMapper,
      boolean detail
  )
  {
    final MSQErrorReport exceptionDetails = getQueryExceptionDetails(msqTaskReportPayload);
    final MSQFault fault = exceptionDetails == null ? null : exceptionDetails.getFault();
    if (exceptionDetails == null || fault == null) {
      return Optional.of(new SqlStatementResult(
          queryId,
          sqlStatementState,
          taskResponse.getStatus().getCreatedTime(),
          null,
          taskResponse.getStatus().getDuration(),
          null,
          DruidException.forPersona(DruidException.Persona.DEVELOPER)
                        .ofCategory(DruidException.Category.UNCATEGORIZED)
                        .build("%s", taskResponse.getStatus().getErrorMsg())
                        .toErrorResponse(),
          detail ? getQueryStagesReport(msqTaskReportPayload) : null,
          detail ? getQueryCounters(msqTaskReportPayload) : null,
          detail ? getQueryWarningDetails(msqTaskReportPayload) : null
      ));
    }

    final String errorMessage = fault.getErrorMessage() == null ? statusPlus.getErrorMsg() : fault.getErrorMessage();
    final String errorCode = fault.getErrorCode() == null ? "unknown" : fault.getErrorCode();

    final Map<String, String> exceptionContext = buildExceptionContext(fault, jsonMapper);
    return Optional.of(new SqlStatementResult(
        queryId,
        sqlStatementState,
        taskResponse.getStatus().getCreatedTime(),
        null,
        taskResponse.getStatus().getDuration(),
        null,
        DruidException.fromFailure(new DruidException.Failure(errorCode)
        {
          @Override
          protected DruidException makeException(DruidException.DruidExceptionBuilder bob)
          {
            DruidException ex = bob.forPersona(DruidException.Persona.USER)
                                   .ofCategory(DruidException.Category.UNCATEGORIZED)
                                   .build(errorMessage);
            ex.withContext(exceptionContext);
            return ex;
          }
        }).toErrorResponse(),
        detail ? getQueryStagesReport(msqTaskReportPayload) : null,
        detail ? getQueryCounters(msqTaskReportPayload) : null,
        detail ? getQueryWarningDetails(msqTaskReportPayload) : null
    ));
  }

  public static Sequence<Object[]> getResultSequence(
      final Frame resultsFrame,
      final FrameReader resultFrameReader,
      final ColumnMappings resultColumnMappings,
      final ResultsContext resultsContext,
      final ObjectMapper jsonMapper
  )
  {
    final Cursor cursor = FrameProcessors.makeCursor(resultsFrame, resultFrameReader);
    final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
    @SuppressWarnings("rawtypes")
    final List<ColumnValueSelector> selectors =
        resultColumnMappings.getMappings()
                            .stream()
                            .map(mapping -> columnSelectorFactory.makeColumnValueSelector(mapping.getQueryColumn()))
                            .collect(Collectors.toList());

    final Iterable<Object[]> retVal = () -> new Iterator<>()
    {
      @Override
      public boolean hasNext()
      {
        return !cursor.isDone();
      }

      @Override
      public Object[] next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        final Object[] row = new Object[resultColumnMappings.size()];
        for (int i = 0; i < row.length; i++) {
          final Object value = selectors.get(i).getObject();
          if (resultsContext.getSqlTypeNames() == null || resultsContext.getSqlResultsContext() == null) {
            // SQL type unknown, or no SQL results context: pass-through as is.
            row[i] = value;
          } else {
            row[i] = SqlResults.coerce(
                jsonMapper,
                resultsContext.getSqlResultsContext(),
                value,
                resultsContext.getSqlTypeNames().get(i),
                resultColumnMappings.getOutputColumnName(i)
            );
          }
        }
        cursor.advance();
        return row;
      }
    };
    return Sequences.simple(retVal);
  }

  @Nullable
  public static MSQStagesReport.Stage getFinalStage(@Nullable MSQTaskReportPayload msqTaskReportPayload)
  {
    if (msqTaskReportPayload == null || msqTaskReportPayload.getStages().getStages() == null) {
      return null;
    }
    int finalStageNumber = msqTaskReportPayload.getStages().getStages().size() - 1;

    for (MSQStagesReport.Stage stage : msqTaskReportPayload.getStages().getStages()) {
      if (stage.getStageNumber() == finalStageNumber) {
        return stage;
      }
    }
    return null;
  }

  @Nullable
  private static MSQErrorReport getQueryExceptionDetails(@Nullable MSQTaskReportPayload payload)
  {
    return payload == null ? null : payload.getStatus().getErrorReport();
  }

  @Nullable
  public static List<MSQErrorReport> getQueryWarningDetails(@Nullable MSQTaskReportPayload payload)
  {
    return payload == null ? null : new ArrayList<>(payload.getStatus().getWarningReports());
  }

  @Nullable
  public static MSQStagesReport getQueryStagesReport(@Nullable MSQTaskReportPayload payload)
  {
    return payload == null ? null : payload.getStages();
  }

  @Nullable
  public static CounterSnapshotsTree getQueryCounters(@Nullable MSQTaskReportPayload payload)
  {
    return payload == null ? null : payload.getCounters();
  }

  @Nullable
  public static MSQTaskReportPayload getPayload(TaskReport.ReportMap reportMap)
  {
    if (reportMap == null) {
      return null;
    }

    Optional<MSQTaskReport> report = reportMap.findReport("multiStageQuery");
    return report.map(MSQTaskReport::getPayload).orElse(null);
  }

  private static Map<String, String> buildExceptionContext(MSQFault fault, ObjectMapper mapper)
  {
    try {
      final Map<String, Object> msqFaultAsMap = new HashMap<>(
          mapper.readValue(
              mapper.writeValueAsBytes(fault),
              JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
          )
      );
      msqFaultAsMap.remove("errorCode");
      msqFaultAsMap.remove("errorMessage");

      final Map<String, String> exceptionContext = new HashMap<>();
      msqFaultAsMap.forEach((key, value) -> exceptionContext.put(key, String.valueOf(value)));

      return exceptionContext;
    }
    catch (Exception e) {
      throw DruidException.defensive("Could not read MSQFault[%s] as a map: [%s]", fault, e.getMessage());
    }
  }
}
