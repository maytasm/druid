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

package org.apache.druid.benchmark.compression;

import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.MappedByteBufferHandler;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.data.ColumnarLongs;
import org.apache.druid.segment.data.CompressedColumnarLongsSupplier;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Run {@link LongCompressionBenchmarkFileGenerator} to generate the required files before running this benchmark
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class LongCompressionBenchmark
{
  @Param("longCompress/")
  private static String dirPath;

  @Param({"enumerate", "zipfLow", "zipfHigh", "sequential", "uniform"})
  private static String file;

  @Param({"auto", "longs"})
  private static String format;

  @Param({"lz4"})
  private static String strategy;

  private Supplier<ColumnarLongs> supplier;

  private MappedByteBufferHandler bufferHandler;

  @Setup
  public void setup() throws Exception
  {
    File dir = new File(dirPath);
    File compFile = new File(dir, file + "-" + strategy + "-" + format);
    bufferHandler = FileUtils.map(compFile);
    ByteBuffer buffer = bufferHandler.get();
    supplier = CompressedColumnarLongsSupplier.fromByteBuffer(buffer, ByteOrder.nativeOrder(), null);
  }

  @TearDown
  public void tearDown()
  {
    bufferHandler.close();
  }

  @Benchmark
  public void readContinuous(Blackhole bh)
  {
    ColumnarLongs columnarLongs = supplier.get();
    int count = columnarLongs.size();
    for (int i = 0; i < count; i++) {
      bh.consume(columnarLongs.get(i));
    }
    columnarLongs.close();
  }

  @Benchmark
  public void readSkipping(Blackhole bh)
  {
    ColumnarLongs columnarLongs = supplier.get();
    int count = columnarLongs.size();
    for (int i = 0; i < count; i += ThreadLocalRandom.current().nextInt(2000)) {
      bh.consume(columnarLongs.get(i));
    }
    columnarLongs.close();
  }

  @Benchmark
  public void readVectorizedContinuous(Blackhole bh)
  {
    long[] vector = new long[QueryContexts.DEFAULT_VECTOR_SIZE];
    ColumnarLongs columnarLongs = supplier.get();
    int count = columnarLongs.size();
    for (int i = 0; i < count; i++) {
      if (i % vector.length == 0) {
        columnarLongs.get(vector, i, Math.min(vector.length, count - i));
      }
      bh.consume(vector[i % vector.length]);
    }
    columnarLongs.close();
  }
}
