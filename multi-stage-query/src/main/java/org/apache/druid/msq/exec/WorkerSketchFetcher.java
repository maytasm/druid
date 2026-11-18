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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.function.TriConsumer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.indexing.error.WorkerRpcFailedFault;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernel;
import org.apache.druid.msq.rpc.SketchEncoding;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.statistics.CompleteKeyStatisticsInformation;
import org.apache.druid.msq.statistics.DelegateOrMinKeyCollectorSnapshot;
import org.apache.druid.msq.statistics.DistinctKeySnapshot;
import org.apache.druid.msq.statistics.KeyCollectorSnapshot;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

/**
 * Queues up fetching sketches from workers and progressively generates partitions boundaries.
 */
public class WorkerSketchFetcher implements AutoCloseable
{
  private static final Logger log = new Logger(WorkerSketchFetcher.class);
  private static final int DEFAULT_THREAD_COUNT = 4;

  private final WorkerClient workerClient;
  private final SketchEncoding sketchEncoding;
  private final WorkerManager workerManager;

  private final boolean retryEnabled;

  private final AtomicReference<Throwable> isError = new AtomicReference<>();
  final ExecutorService executorService;


  public WorkerSketchFetcher(
      WorkerClient workerClient,
      WorkerManager workerManager,
      boolean retryEnabled,
      SketchEncoding sketchEncoding
  )
  {
    this.workerClient = workerClient;
    this.sketchEncoding = sketchEncoding;
    this.executorService = Execs.multiThreaded(DEFAULT_THREAD_COUNT, "SketchFetcherThreadPool-%d");
    this.workerManager = workerManager;
    this.retryEnabled = retryEnabled;
  }

  /**
   * Fetches the full {@link ClusterByStatisticsCollector} from all workers and generates partition boundaries from them.
   * This is faster than fetching them timechunk by timechunk but the collector will be downsampled till it can fit
   * on the controller, resulting in less accurate partition boundries.
   */
  public void inMemoryFullSketchMerging(
      Consumer<Consumer<ControllerQueryKernel>> kernelActions,
      StageId stageId,
      Set<String> taskIds,
      TriConsumer<ControllerQueryKernel, Integer, MSQFault> retryOperation
  )
  {
    final FetchStats fetchStats = new FetchStats(stageId, "PARALLEL", taskIds.size());

    for (String taskId : taskIds) {
      try {
        int workerNumber = workerManager.getWorkerNumber(taskId);
        final FetchRecord fetchRecord = fetchStats.newRecord(taskId, workerNumber);
        executorService.submit(() -> {
          fetchStatsFromWorker(
              kernelActions,
              () -> workerClient.fetchClusterByStatisticsSnapshot(taskId, stageId, sketchEncoding),
              taskId,
              (kernel, snapshot) ->
                  kernel.mergeClusterByStatisticsCollectorForAllTimeChunks(stageId, workerNumber, snapshot),
              retryOperation,
              fetchRecord
          );
        });
      }
      catch (RejectedExecutionException rejectedExecutionException) {
        if (isError.get() == null) {
          throw rejectedExecutionException;
        } else {
          // throw worker error exception
          throw new ISE("Unable to fetch partitions %s", isError.get());
        }
      }
    }
  }

  private void fetchStatsFromWorker(
      Consumer<Consumer<ControllerQueryKernel>> kernelActions,
      Supplier<ListenableFuture<ClusterByStatisticsSnapshot>> fetchStatsSupplier,
      String taskId,
      BiConsumer<ControllerQueryKernel, ClusterByStatisticsSnapshot> successKernelOperation,
      TriConsumer<ControllerQueryKernel, Integer, MSQFault> retryOperation,
      FetchRecord fetchRecord
  )
  {
    fetchRecord.startedNanos = System.nanoTime();
    try {
      doFetchStatsFromWorker(
          kernelActions,
          fetchStatsSupplier,
          taskId,
          successKernelOperation,
          retryOperation,
          fetchRecord
      );
    }
    finally {
      fetchRecord.finish();
    }
  }

  private void doFetchStatsFromWorker(
      Consumer<Consumer<ControllerQueryKernel>> kernelActions,
      Supplier<ListenableFuture<ClusterByStatisticsSnapshot>> fetchStatsSupplier,
      String taskId,
      BiConsumer<ControllerQueryKernel, ClusterByStatisticsSnapshot> successKernelOperation,
      TriConsumer<ControllerQueryKernel, Integer, MSQFault> retryOperation,
      FetchRecord fetchRecord
  )
  {
    if (isError.get() != null) {
      executorService.shutdownNow();
      return;
    }
    int worker = workerManager.getWorkerNumber(taskId);
    if (worker == WorkerManager.UNKNOWN_WORKER_NUMBER) {
      log.info("Task[%s] is no longer the latest task for worker[%d]. Skipping fetch.", taskId, worker);
      return;
    }

    try {
      // since this is running another in thread, hence the controller thread does not block; hence we need not call with failure handler.
      workerManager.waitForWorkers(ImmutableSet.of(worker));
    }
    catch (InterruptedException interruptedException) {
      isError.compareAndSet(null, interruptedException);
      executorService.shutdownNow();
      return;
    }

    // if the task is not the latest task, it must have retried.
    if (!workerManager.isWorkerActive(taskId)) {
      log.info("Task[%s] is no longer the latest task for worker[%d]. Skipping fetch.", taskId, worker);
      return;
    }

    fetchRecord.sentNanos = System.nanoTime();
    ListenableFuture<ClusterByStatisticsSnapshot> fetchFuture = fetchStatsSupplier.get();

    SettableFuture<Boolean> kernelActionFuture = SettableFuture.create();

    Futures.addCallback(fetchFuture, new FutureCallback<>()
    {
      @Override
      public void onSuccess(@Nullable ClusterByStatisticsSnapshot result)
      {
        fetchRecord.onReceived(result);
        try {
          kernelActions.accept((queryKernel) -> {
            try {
              fetchRecord.mergeStartNanos = System.nanoTime();
              successKernelOperation.accept(queryKernel, result);
              fetchRecord.mergeEndNanos = System.nanoTime();
              // we do not want to have too many key collector sketches in the event queue as that cause memory issues
              // blocking the executor service thread until the kernel operation is finished.
              // so we would have utmost DEFAULT_THREAD_COUNT number of sketches in the queue.
              kernelActionFuture.set(true);
            }
            catch (Exception e) {
              failFutureAndShutDownExecutorService(e, taskId, kernelActionFuture);
            }
          });
        }
        catch (Exception e) {
          failFutureAndShutDownExecutorService(e, taskId, kernelActionFuture);
        }

      }

      @Override
      public void onFailure(Throwable t)
      {
        fetchRecord.onFetchFailed();

        if (retryEnabled) {
          //add to retry queue
          try {
            kernelActions.accept((kernel) -> {
              try {
                retryOperation.accept(kernel, worker, new WorkerRpcFailedFault(taskId, t.toString()));
                kernelActionFuture.set(false);

              }
              catch (Exception e) {
                failFutureAndShutDownExecutorService(e, taskId, kernelActionFuture);
              }
            });
            kernelActionFuture.set(false);
          }
          catch (Exception e) {
            failFutureAndShutDownExecutorService(e, taskId, kernelActionFuture);
          }
        } else {
          failFutureAndShutDownExecutorService(t, taskId, kernelActionFuture);
        }

      }
    }, MoreExecutors.directExecutor());

    FutureUtils.getUnchecked(kernelActionFuture, true);
  }

  private void failFutureAndShutDownExecutorService(
      Throwable t,
      String taskId,
      SettableFuture<Boolean> kernelActionFuture
  )
  {
    if (isError.compareAndSet(null, t)) {
      log.error(t, "Failed while fetching stats from task[%s]", taskId);
    }
    executorService.shutdownNow();
    kernelActionFuture.setException(t);
  }

  /**
   * Fetches cluster statistics from all workers and generates partition boundaries from them one time chunk at a time.
   * This takes longer due to the overhead of fetching sketches, however, this prevents any loss in accuracy from
   * down sampling on the controller.
   */
  public void sequentialTimeChunkMerging(
      Consumer<Consumer<ControllerQueryKernel>> kernelActions,
      CompleteKeyStatisticsInformation completeKeyStatisticsInformation,
      StageId stageId,
      Set<String> tasks,
      TriConsumer<ControllerQueryKernel, Integer, MSQFault> retryOperation
  )
  {
    if (!completeKeyStatisticsInformation.isComplete()) {
      throw new ISE("All worker partial key information not received for stage[%d]", stageId.getStageNumber());
    }

    final Set<String> noBoundaries = new HashSet<>(tasks);
    final FetchStats fetchStats =
        new FetchStats(stageId, "SEQUENTIAL", countFetches(completeKeyStatisticsInformation, tasks));
    completeKeyStatisticsInformation.getTimeSegmentVsWorkerMap().forEach((timeChunk, wks) -> {

      for (String taskId : tasks) {
        int workerNumber = workerManager.getWorkerNumber(taskId);
        if (wks.contains(workerNumber)) {
          noBoundaries.remove(taskId);
          final FetchRecord fetchRecord = fetchStats.newRecord(taskId, workerNumber);
          executorService.submit(() -> {
            fetchStatsFromWorker(
                kernelActions,
                () -> workerClient.fetchClusterByStatisticsSnapshotForTimeChunk(
                    taskId,
                    new StageId(stageId.getQueryId(), stageId.getStageNumber()),
                    timeChunk,
                    sketchEncoding
                ),
                taskId,
                (kernel, snapshot) -> kernel.mergeClusterByStatisticsCollectorForTimeChunk(
                    stageId,
                    workerNumber,
                    timeChunk,
                    snapshot
                ),
                retryOperation,
                fetchRecord
            );

          });
        }
      }
    });

    // if the worker did not get any records, update the state of the worker
    for (String taskId : noBoundaries) {
      kernelActions.accept(
          kernel -> {
            final int workerNumber = workerManager.getWorkerNumber(taskId);
            kernel.mergeClusterByStatisticsCollectorForAllTimeChunks(
                stageId,
                workerNumber,
                ClusterByStatisticsSnapshot.empty()
            );
          });
    }
  }


  /**
   * Returns {@link Throwable} if error, else null
   */
  public Throwable getError()
  {
    return isError.get();
  }


  @Override
  public void close()
  {
    try {
      executorService.shutdownNow();
    }
    catch (Throwable suppressed) {
      log.warn(suppressed, "Error while shutting down WorkerSketchFetcher");
    }
  }

  /**
   * Number of fetches {@link #sequentialTimeChunkMerging} submits: one for each time chunk of each worker that has it.
   */
  private int countFetches(
      final CompleteKeyStatisticsInformation completeKeyStatisticsInformation,
      final Set<String> tasks
  )
  {
    int count = 0;
    for (final Set<Integer> workers : completeKeyStatisticsInformation.getTimeSegmentVsWorkerMap().values()) {
      for (final String taskId : tasks) {
        if (workers.contains(workerManager.getWorkerNumber(taskId))) {
          count++;
        }
      }
    }
    return count;
  }

  /**
   * Diagnostic record of one statistics fetch: how long each step took, and what the snapshot held. The fetch thread,
   * the HTTP callback thread and the controller thread write its fields in turn.
   */
  private static class FetchRecord
  {
    private final FetchStats stats;
    private final String taskId;
    private final int workerNumber;
    private final long submittedNanos = System.nanoTime();
    private volatile long startedNanos;
    private volatile long sentNanos;
    private volatile long receivedNanos;
    private volatile long mergeStartNanos;
    private volatile long mergeEndNanos;
    private volatile boolean fetchFailed;

    /**
     * Highest spaceReductionFactor among the snapshot's distinct-key collectors, or -1 if it has none.
     */
    private volatile int spaceReductionFactor = -1;
    private volatile long keys;
    private volatile long keyBytes;

    private FetchRecord(final FetchStats stats, final String taskId, final int workerNumber)
    {
      this.stats = stats;
      this.taskId = taskId;
      this.workerNumber = workerNumber;
    }

    /**
     * Records when the snapshot arrived and what it holds. Never throws: this is diagnostics, and a failure here must
     * not stop the snapshot from being merged.
     */
    private void onReceived(@Nullable final ClusterByStatisticsSnapshot snapshot)
    {
      receivedNanos = System.nanoTime();
      try {
        describe(snapshot);
      }
      catch (RuntimeException e) {
        log.debug(e, "Could not describe the statistics snapshot from task[%s].", taskId);
      }
    }

    private void describe(@Nullable final ClusterByStatisticsSnapshot snapshot)
    {
      final Map<Long, ClusterByStatisticsSnapshot.Bucket> buckets = snapshot == null ? null : snapshot.getBuckets();
      if (buckets == null) {
        return;
      }

      int factor = -1;
      long numKeys = 0;
      long numKeyBytes = 0;
      for (final ClusterByStatisticsSnapshot.Bucket bucket : buckets.values()) {
        KeyCollectorSnapshot keyCollectorSnapshot = bucket.getKeyCollectorSnapshot();
        if (keyCollectorSnapshot instanceof DelegateOrMinKeyCollectorSnapshot) {
          final DelegateOrMinKeyCollectorSnapshot<?> delegateOrMinSnapshot =
              (DelegateOrMinKeyCollectorSnapshot<?>) keyCollectorSnapshot;
          if (delegateOrMinSnapshot.getSnapshot() == null) {
            // Holds only a min key.
            if (delegateOrMinSnapshot.getMinKey() != null) {
              numKeys++;
              numKeyBytes += delegateOrMinSnapshot.getMinKey().array().length;
            }
            continue;
          }
          keyCollectorSnapshot = delegateOrMinSnapshot.getSnapshot();
        }

        if (keyCollectorSnapshot instanceof DistinctKeySnapshot) {
          final DistinctKeySnapshot distinctKeySnapshot = (DistinctKeySnapshot) keyCollectorSnapshot;
          factor = Math.max(factor, distinctKeySnapshot.getSpaceReductionFactor());
          for (final SerializablePair<RowKey, Long> key : distinctKeySnapshot.getKeys()) {
            numKeys++;
            numKeyBytes += key.lhs.array().length;
          }
        }
      }

      spaceReductionFactor = factor;
      keys = numKeys;
      keyBytes = numKeyBytes;
    }

    private void onFetchFailed()
    {
      receivedNanos = System.nanoTime();
      fetchFailed = true;
    }

    /**
     * Logs this record, and the summary if it is the stage's last fetch. Never throws, so it can't hide an exception
     * from the fetch or stop the fetch thread.
     */
    private void finish()
    {
      try {
        stats.onFinished(this);
      }
      catch (RuntimeException e) {
        log.debug(e, "Could not log the statistics fetch from task[%s].", taskId);
      }
    }

    private boolean isMerged()
    {
      return mergeEndNanos != 0;
    }

    private String outcome()
    {
      if (isMerged()) {
        return "merged";
      } else if (fetchFailed) {
        return "fetchFailed";
      } else if (receivedNanos != 0) {
        return "notMerged";
      } else if (sentNanos != 0) {
        return "noResponse";
      } else {
        return "notSent";
      }
    }

    /**
     * Time waiting for a fetch thread.
     */
    private double queuedMs()
    {
      return millis(submittedNanos, startedNanos);
    }

    /**
     * Time in {@link WorkerManager#waitForWorkers}.
     */
    private double waitForWorkerMs()
    {
      return millis(startedNanos, sentNanos);
    }

    /**
     * Request sent to snapshot decoded: the worker's handling, response compression, transfer and decoding.
     */
    private double fetchMs()
    {
      return millis(sentNanos, receivedNanos);
    }

    /**
     * Time waiting for the controller thread to start the merge.
     */
    private double mergeQueueMs()
    {
      return millis(receivedNanos, mergeStartNanos);
    }

    private double mergeMs()
    {
      return millis(mergeStartNanos, mergeEndNanos);
    }

    private static double millis(final long startNanos, final long endNanos)
    {
      return startNanos == 0 || endNanos == 0 ? -1 : (endNanos - startNanos) / 1e6;
    }
  }

  /**
   * Diagnostic: collects the {@link FetchRecord} of one stage's statistics fetches. Logs a line per fetch, and a
   * summary once all fetches have finished.
   */
  private static class FetchStats
  {
    private final StageId stageId;
    private final String mode;
    private final int expectedFetches;
    private final long createdNanos = System.nanoTime();

    // Guarded by "this".
    private final List<FetchRecord> finished = new ArrayList<>();

    private FetchStats(final StageId stageId, final String mode, final int expectedFetches)
    {
      this.stageId = stageId;
      this.mode = mode;
      this.expectedFetches = expectedFetches;
    }

    private FetchRecord newRecord(final String taskId, final int workerNumber)
    {
      return new FetchRecord(this, taskId, workerNumber);
    }

    private void onFinished(final FetchRecord record)
    {
      log.info(
          "Statistics fetch for stage[%s] worker[%d] task[%s]: outcome[%s], spaceReductionFactor[%d], keys[%d], "
          + "keyBytes[%d], queuedMs[%.1f], waitForWorkerMs[%.1f], fetchMs[%.1f], mergeQueueMs[%.1f], mergeMs[%.1f].",
          stageId,
          record.workerNumber,
          record.taskId,
          record.outcome(),
          record.spaceReductionFactor,
          record.keys,
          record.keyBytes,
          record.queuedMs(),
          record.waitForWorkerMs(),
          record.fetchMs(),
          record.mergeQueueMs(),
          record.mergeMs()
      );

      final List<FetchRecord> records;
      synchronized (this) {
        finished.add(record);
        if (finished.size() != expectedFetches) {
          return;
        }
        records = new ArrayList<>(finished);
      }

      log.info("%s", summarize(records));
    }

    private String summarize(final List<FetchRecord> records)
    {
      final List<FetchRecord> merged = new ArrayList<>();
      final Map<Integer, Integer> factorHistogram = new TreeMap<>();
      int maxFactor = -1;
      for (final FetchRecord record : records) {
        if (record.isMerged()) {
          merged.add(record);
          factorHistogram.merge(record.spaceReductionFactor, 1, Integer::sum);
          maxFactor = Math.max(maxFactor, record.spaceReductionFactor);
        }
      }

      // The merged collector reaches the highest factor any snapshot has, and each extra level of downsampling keeps
      // a key with probability 1/2. So this estimates what the snapshots would hold if they had been sent at that
      // factor.
      double keysAtMaxFactor = 0;
      double keyBytesAtMaxFactor = 0;
      for (final FetchRecord record : merged) {
        if (record.spaceReductionFactor >= 0) {
          final double keepFraction = Math.pow(2, record.spaceReductionFactor - maxFactor);
          keysAtMaxFactor += record.keys * keepFraction;
          keyBytesAtMaxFactor += record.keyBytes * keepFraction;
        }
      }

      return StringUtils.format(
          "Statistics fetch summary for stage[%s]: mode[%s], fetchThreads[%d], fetches[%d], merged[%d], "
          + "elapsedMs[%.0f], spaceReductionFactor histogram%s, keys[%s], keyBytes[%s], "
          + "estimated at max factor[keys=%.0f keyBytes=%.0f], queuedMs[%s], waitForWorkerMs[%s], fetchMs[%s], "
          + "fetchMs vs keyBytes[%s], mergeQueueMs[%s], mergeMs[%s].",
          stageId,
          mode,
          DEFAULT_THREAD_COUNT,
          records.size(),
          merged.size(),
          (System.nanoTime() - createdNanos) / 1e6,
          factorHistogram,
          distribution(merged, record -> record.keys, "%.0f"),
          distribution(merged, record -> record.keyBytes, "%.0f"),
          keysAtMaxFactor,
          keyBytesAtMaxFactor,
          distribution(merged, FetchRecord::queuedMs, "%.1f"),
          distribution(merged, FetchRecord::waitForWorkerMs, "%.1f"),
          distribution(merged, FetchRecord::fetchMs, "%.1f"),
          fitFetchMsToKeyMegabytes(merged),
          distribution(merged, FetchRecord::mergeQueueMs, "%.1f"),
          distribution(merged, FetchRecord::mergeMs, "%.1f")
      );
    }

    private static String distribution(
        final List<FetchRecord> records,
        final ToDoubleFunction<FetchRecord> valueFn,
        final String valueFormat
    )
    {
      if (records.isEmpty()) {
        return "n/a";
      }

      final double[] values = records.stream().mapToDouble(valueFn).sorted().toArray();
      double sum = 0;
      for (final double value : values) {
        sum += value;
      }

      return StringUtils.format(
          "sum=" + valueFormat + " p50=" + valueFormat + " p90=" + valueFormat + " max=" + valueFormat,
          sum,
          percentile(values, 0.5),
          percentile(values, 0.9),
          values[values.length - 1]
      );
    }

    private static double percentile(final double[] sortedValues, final double fraction)
    {
      final int index = (int) Math.ceil(fraction * sortedValues.length) - 1;
      return sortedValues[Math.max(0, Math.min(sortedValues.length - 1, index))];
    }

    /**
     * Least-squares fit of fetch time against snapshot size: the intercept is the per-fetch cost, the slope the cost
     * per MB of keys.
     */
    private static String fitFetchMsToKeyMegabytes(final List<FetchRecord> records)
    {
      final int n = records.size();
      double sumX = 0;
      double sumY = 0;
      double sumXX = 0;
      double sumYY = 0;
      double sumXY = 0;
      for (final FetchRecord record : records) {
        final double x = record.keyBytes / 1e6;
        final double y = record.fetchMs();
        sumX += x;
        sumY += y;
        sumXX += x * x;
        sumYY += y * y;
        sumXY += x * y;
      }

      final double varianceX = n * sumXX - sumX * sumX;
      if (n < 2 || varianceX <= 0) {
        return "n/a";
      }

      final double covariance = n * sumXY - sumX * sumY;
      final double slope = covariance / varianceX;
      final double intercept = (sumY - slope * sumX) / n;
      final double varianceY = n * sumYY - sumY * sumY;
      final double r = varianceY > 0 ? covariance / Math.sqrt(varianceX * varianceY) : 0;
      return StringUtils.format("ms=%.1f+%.1f*MB r=%.2f", intercept, slope, r);
    }
  }
}
