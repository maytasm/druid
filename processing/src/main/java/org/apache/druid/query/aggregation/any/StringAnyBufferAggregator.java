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

package org.apache.druid.query.aggregation.any;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;

import java.nio.ByteBuffer;

public class StringAnyBufferAggregator implements BufferAggregator
{
  private static final byte BYTE_FLAG_IS_NOT_SET = 0;
  private static final byte BYTE_FLAG_IS_SET = 1;
  private static final int NULL_STRING_LENGTH = -1;
  private static final int IS_FOUND_FLAG_OFFSET = 0;
  private static final int STRING_LENGTH_OFFSET = IS_FOUND_FLAG_OFFSET + Byte.BYTES;
  private static final int FOUND_VALUE_OFFSET = STRING_LENGTH_OFFSET + Integer.BYTES;

  private final BaseObjectColumnValueSelector valueSelector;
  private final int maxStringBytes;

  public StringAnyBufferAggregator(BaseObjectColumnValueSelector valueSelector, int maxStringBytes)
  {
    this.valueSelector = valueSelector;
    this.maxStringBytes = maxStringBytes;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.put(position + IS_FOUND_FLAG_OFFSET, BYTE_FLAG_IS_NOT_SET);
    buf.putInt(position + STRING_LENGTH_OFFSET, NULL_STRING_LENGTH);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (buf.get(position + IS_FOUND_FLAG_OFFSET) == BYTE_FLAG_IS_NOT_SET) {
      final Object object = valueSelector.getObject();
      String foundValue = DimensionHandlerUtils.convertObjectToString(object);
      if (foundValue != null) {
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position + FOUND_VALUE_OFFSET);
        mutationBuffer.limit(position + FOUND_VALUE_OFFSET + maxStringBytes);
        final int len = StringUtils.toUtf8WithLimit(foundValue, mutationBuffer);
        mutationBuffer.putInt(position + STRING_LENGTH_OFFSET, len);
      } else {
        buf.putInt(position + STRING_LENGTH_OFFSET, NULL_STRING_LENGTH);
      }
      buf.put(position + IS_FOUND_FLAG_OFFSET, BYTE_FLAG_IS_SET);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer copyBuffer = buf.duplicate();
    copyBuffer.position(position + STRING_LENGTH_OFFSET);
    int stringSizeBytes = copyBuffer.getInt();
    if (stringSizeBytes >= 0) {
      byte[] valueBytes = new byte[stringSizeBytes];
      copyBuffer.get(valueBytes, 0, stringSizeBytes);
      return StringUtils.fromUtf8(valueBytes);
    } else {
      return null;
    }
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringAnyBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringAnyBufferAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringAnyBufferAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // no-op
  }
}
