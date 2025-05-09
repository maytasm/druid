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

package org.apache.druid.segment.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.serde.MetaSerdeHelper;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Compression of metrics is done by using a combination of {@link CompressionStrategy}
 * and Encoding(such as {@link LongEncodingStrategy} for type Long). CompressionStrategy is unaware of the data type
 * and is based on byte operations. It must compress and decompress in block of bytes. Encoding refers to compression
 * method relies on data format, so a different set of Encodings exist for each data type.
 * <p>
 * Storage Format :
 * Byte 1 : version (currently 0x02)
 * Byte 2 - 5 : number of values
 * Byte 6 - 9 : size per block (even if block format isn't used, this is needed for backward compatibility)
 * Byte 10 : compression strategy (contains a flag if there's an encoding byte, see below for how the flag is defined)
 * Byte 11(optional) : encoding type
 * <p>
 * Encoding specific header (described below)
 * <p>
 * Block related header (if block compression is used, described in GenericIndexed)
 * <p>
 * Values
 */
public class CompressionFactory
{
  private CompressionFactory()
  {
    // No instantiation
  }

  public static final LongEncodingStrategy DEFAULT_LONG_ENCODING_STRATEGY = LongEncodingStrategy.LONGS;

  // encoding format for segments created prior to the introduction of encoding formats
  public static final LongEncodingFormat LEGACY_LONG_ENCODING_FORMAT = LongEncodingFormat.LONGS;

  /**
   * Delta Encoding Header v1:
   * Byte 1 : version
   * Byte 2 - 9 : base value
   * Byte 10 - 13 : number of bits per value
   */
  public static final byte DELTA_ENCODING_VERSION = 0x1;

  /**
   * Table Encoding Header v1 :
   * Byte 1 : version
   * Byte 2 - 5 : table size
   * Byte 6 - (6 + 8 * table size - 1) : table of encoding, where the ith 8-byte value is encoded as i
   */
  public static final byte TABLE_ENCODING_VERSION = 0x1;

  public static final int MAX_TABLE_SIZE = 256;

  /*
   * There is no header or version for Longs encoding for backward compatibility
   */

  /*
   * This is the flag mechanism for determine whether an encoding byte exist in the header. This is needed for
   * backward compatibility, since segment created prior to the introduction of encoding formats does not have the
   * encoding strategy byte. The flag is encoded in the compression strategy byte using the setEncodingFlag and
   * clearEncodingFlag function.
   */

  // 0xFE(-2) should be the smallest valid compression strategy id
  private static byte FLAG_BOUND = (byte) 0xFE;
  // 126 is the value here since -2 - 126 = -128, which is the lowest byte value
  private static int FLAG_VALUE = 126;

  public static boolean hasEncodingFlag(byte strategyId)
  {
    return strategyId < FLAG_BOUND;
  }

  public static byte setEncodingFlag(byte strategyId)
  {
    return hasEncodingFlag(strategyId) ? strategyId : (byte) (strategyId - FLAG_VALUE);
  }

  public static byte clearEncodingFlag(byte strategyId)
  {
    return hasEncodingFlag(strategyId) ? (byte) (strategyId + FLAG_VALUE) : strategyId;
  }

  /**
   * The compression of decompression of encodings are separated into different enums. EncodingStrategy refers to the
   * strategy used to encode the data, and EncodingFormat refers to the format the data is encoded in. Note there is not
   * necessarily an one-to-one mapping between to two. For instance, the AUTO LongEncodingStrategy scans the data once
   * and decide on which LongEncodingFormat to use based on data property, so it's possible for the EncodingStrategy to
   * write in any of the LongEncodingFormat. On the other hand, there are no LongEncodingStrategy that always write in
   * TABLE LongEncodingFormat since it only works for data with low cardinality.
   */
  public enum LongEncodingStrategy
  {
    /**
     * AUTO strategy scans all values once before encoding them. It stores the value cardinality and maximum offset
     * of the values to determine whether to use DELTA, TABLE, or LONGS format.
     */
    AUTO,

    /**
     * LONGS strategy always encode the values using LONGS format
     */
    LONGS;

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(this.name());
    }

    @JsonCreator
    public static LongEncodingStrategy fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }
  }

  public enum LongEncodingFormat
  {
    /**
     * DELTA format encodes a series of longs by finding the smallest value first, and stores all values
     * as offset to the smallest value. The maximum value is also found to calculate how many bits are required
     * to store each offset using {@link VSizeLongSerde}.
     */
    DELTA((byte) 0x0) {
      @Override
      public LongEncodingReader getReader(ByteBuffer buffer, ByteOrder order)
      {
        return new DeltaLongEncodingReader(buffer);
      }
    },
    /**
     * TABLE format encodes a series of longs by mapping each unique value to an id, and string the id with the
     * minimum number of bits similar to how DELTA stores offset. TABLE format is only applicable to values with
     * less unique values than {@link CompressionFactory#MAX_TABLE_SIZE}.
     */
    TABLE((byte) 0x1) {
      @Override
      public LongEncodingReader getReader(ByteBuffer buffer, ByteOrder order)
      {
        return new TableLongEncodingReader(buffer);
      }
    },
    /**
     * LONGS format encodes longs as is, using 8 bytes for each value.
     */
    LONGS((byte) 0xFF) {
      @Override
      public LongEncodingReader getReader(ByteBuffer buffer, ByteOrder order)
      {
        return new LongsLongEncodingReader(buffer, order);
      }
    };

    final byte id;

    LongEncodingFormat(byte id)
    {
      this.id = id;
    }

    public byte getId()
    {
      return id;
    }

    static final Map<Byte, LongEncodingFormat> ID_MAP = new HashMap<>();

    static {
      for (LongEncodingFormat format : LongEncodingFormat.values()) {
        ID_MAP.put(format.getId(), format);
      }
    }

    public abstract LongEncodingReader getReader(ByteBuffer buffer, ByteOrder order);

    public static LongEncodingFormat forId(byte id)
    {
      return ID_MAP.get(id);
    }
  }

  /**
   * This writer output encoded values to the given ByteBuffer or OutputStream. {@link #setBuffer(ByteBuffer)} or
   * {@link #setOutputStream(WriteOutBytes)} must be called before any value is written, and {@link #flush()} must
   * be called before calling setBuffer or setOutputStream again to set another output.
   */
  public interface LongEncodingWriter
  {
    /**
     * Data will be written starting from current position of the buffer, and the position of the buffer will be
     * updated as content is written.
     */
    void setBuffer(ByteBuffer buffer);

    void setOutputStream(WriteOutBytes output);

    void write(long value) throws IOException;

    @SuppressWarnings("unused")
    default void write(long[] values, int offset, int length) throws IOException
    {
      for (int i = offset; i < length; ++i) {
        write(values[i]);
      }
    }

    /**
     * Flush the unwritten content to the current output.
     */
    void flush() throws IOException;

    /**
     * Output the header values of the associating encoding format to the given outputStream. The header also include
     * bytes for compression strategy and encoding format(optional) as described above in Compression Storage Format.
     */
    void putMeta(ByteBuffer metaOut, CompressionStrategy strategy);

    int metaSize();

    /**
     * Get the number of values that can be encoded into each block for the given block size in bytes
     */
    int getBlockSize(int bytesPerBlock);

    /**
     * Get the number of bytes required to encoding the given number of values
     */
    int getNumBytes(int values);
  }

  static <T> MetaSerdeHelper.FieldWriter<T> longEncodingWriter(
      Function<T, LongEncodingWriter> getWriter,
      Function<T, CompressionStrategy> getCompressionStrategy
  )
  {
    return new MetaSerdeHelper.FieldWriter<>()
    {
      @Override
      public void writeTo(ByteBuffer buffer, T x)
      {
        getWriter.apply(x).putMeta(buffer, getCompressionStrategy.apply(x));
      }

      @Override
      public int size(T x)
      {
        return getWriter.apply(x).metaSize();
      }
    };
  }

  public interface LongEncodingReader
  {
    void setBuffer(ByteBuffer buffer);

    long read(int index);

    void read(long[] out, int outPosition, int startIndex, int length);

    int read(long[] out, int outPosition, int[] indexes, int length, int indexOffset, int limit);

    /**
     * Duplicates this reader, creating a new reader that does not share any state. Important to achieve thread-safety,
     * because a common pattern is to duplicate a reader multiple times and then call {@link #setBuffer} on the
     * various duplicates.
     */
    LongEncodingReader duplicate();

    LongEncodingStrategy getStrategy();
  }

  /**
   * Reads a column from a {@link ByteBuffer}, possibly using additional secondary files from a
   * {@link SmooshedFileMapper}.
   *
   * @param totalSize      number of rows in the column
   * @param sizePer        number of values per compression buffer, for compressed columns
   * @param fromBuffer     primary buffer to read from
   * @param order          byte order
   * @param encodingFormat encoding of each long value
   * @param strategy       compression strategy, for compressed columns
   * @param smooshMapper   required for reading version 2 (multi-file) indexed. May be null if you know you are reading
   *                       a single-file column. Generally, this should only be null in tests, not production code.
   */
  public static Supplier<ColumnarLongs> getLongSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer fromBuffer,
      ByteOrder order,
      LongEncodingFormat encodingFormat,
      CompressionStrategy strategy,
      @Nullable SmooshedFileMapper smooshMapper
  )
  {
    if (strategy == CompressionStrategy.NONE) {
      return new EntireLayoutColumnarLongsSupplier(totalSize, encodingFormat.getReader(fromBuffer, order));
    } else {
      return new BlockLayoutColumnarLongsSupplier(
          totalSize,
          sizePer,
          fromBuffer,
          order,
          encodingFormat.getReader(fromBuffer, order),
          strategy,
          smooshMapper
      );
    }
  }

  public static ColumnarLongsSerializer getLongSerializer(
      String columnName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ByteOrder order,
      LongEncodingStrategy encodingStrategy,
      CompressionStrategy compressionStrategy,
      Closer closer
  )
  {
    if (encodingStrategy == LongEncodingStrategy.AUTO) {
      return new IntermediateColumnarLongsSerializer(
          columnName,
          segmentWriteOutMedium,
          filenameBase,
          order,
          compressionStrategy,
          closer
      );
    } else if (encodingStrategy == LongEncodingStrategy.LONGS) {
      if (compressionStrategy == CompressionStrategy.NONE) {
        return new EntireLayoutColumnarLongsSerializer(
            columnName,
            segmentWriteOutMedium,
            new LongsLongEncodingWriter(order)
        );
      } else {
        return new BlockLayoutColumnarLongsSerializer(
            columnName,
            segmentWriteOutMedium,
            filenameBase,
            order,
            new LongsLongEncodingWriter(order),
            compressionStrategy,
            GenericIndexedWriter.MAX_FILE_SIZE,
            closer
        );
      }
    } else {
      throw new IAE("unknown encoding strategy : %s", encodingStrategy.toString());
    }
  }

  // Float currently does not support any encoding types, and stores values as 4 byte float

  /**
   * Reads a column from a {@link ByteBuffer}, possibly using additional secondary files from a
   * {@link SmooshedFileMapper}.
   *
   * @param totalSize    number of rows in the column
   * @param sizePer      number of values per compression buffer, for compressed columns
   * @param fromBuffer   primary buffer to read from
   * @param order        byte order
   * @param strategy     compression strategy, for compressed columns
   * @param smooshMapper required for reading version 2 (multi-file) indexed. May be null if you know you are reading
   *                     a single-file column. Generally, this should only be null in tests, not production code.
   */
  public static Supplier<ColumnarFloats> getFloatSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer fromBuffer,
      ByteOrder order,
      CompressionStrategy strategy,
      @Nullable SmooshedFileMapper smooshMapper
  )
  {
    if (strategy == CompressionStrategy.NONE) {
      return new EntireLayoutColumnarFloatsSupplier(totalSize, fromBuffer, order);
    } else {
      return new BlockLayoutColumnarFloatsSupplier(totalSize, sizePer, fromBuffer, order, strategy, smooshMapper);
    }
  }

  public static ColumnarFloatsSerializer getFloatSerializer(
      String columnName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ByteOrder order,
      CompressionStrategy compressionStrategy,
      Closer closer
  )
  {
    if (compressionStrategy == CompressionStrategy.NONE) {
      return new EntireLayoutColumnarFloatsSerializer(columnName, segmentWriteOutMedium, order);
    } else {
      return new BlockLayoutColumnarFloatsSerializer(
          columnName,
          segmentWriteOutMedium,
          filenameBase,
          order,
          compressionStrategy,
          GenericIndexedWriter.MAX_FILE_SIZE,
          closer
      );
    }
  }

  /**
   * Reads a column from a {@link ByteBuffer}, possibly using additional secondary files from a
   * {@link SmooshedFileMapper}.
   *
   * @param totalSize    number of rows in the column
   * @param sizePer      number of values per compression buffer, for compressed columns
   * @param fromBuffer   primary buffer to read from
   * @param byteOrder    byte order
   * @param strategy     compression strategy, for compressed columns
   * @param smooshMapper required for reading version 2 (multi-file) indexed. May be null if you know you are reading
   *                     a single-file column. Generally, this should only be null in tests, not production code.
   */
  public static Supplier<ColumnarDoubles> getDoubleSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer fromBuffer,
      ByteOrder byteOrder,
      CompressionStrategy strategy,
      SmooshedFileMapper smooshMapper
  )
  {
    if (strategy == CompressionStrategy.NONE) {
      return new EntireLayoutColumnarDoublesSupplier(totalSize, fromBuffer, byteOrder);
    } else {
      return new BlockLayoutColumnarDoublesSupplier(
          totalSize,
          sizePer,
          fromBuffer,
          byteOrder,
          strategy,
          smooshMapper
      );
    }
  }

  public static ColumnarDoublesSerializer getDoubleSerializer(
      String columnName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ByteOrder byteOrder,
      CompressionStrategy compression,
      Closer closer
  )
  {
    if (compression == CompressionStrategy.NONE) {
      return new EntireLayoutColumnarDoublesSerializer(columnName, segmentWriteOutMedium, byteOrder);
    } else {
      return new BlockLayoutColumnarDoublesSerializer(
          columnName,
          segmentWriteOutMedium,
          filenameBase,
          byteOrder,
          compression,
          GenericIndexedWriter.MAX_FILE_SIZE,
          closer
      );
    }
  }
}
