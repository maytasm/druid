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

package org.apache.druid.query.extraction;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

/**
 * Functional extraction uses a function to find the new value.
 * null values in the range can either retain the domain's value, or replace the null value with "replaceMissingValueWith"
 */
public abstract class FunctionalExtraction extends DimExtractionFn
{
  private final boolean retainMissingValue;
  private final String replaceMissingValueWith;
  private final Function<String, String> extractionFunction;
  private final ExtractionType extractionType;

  /**
   * The general constructor which handles most of the logic for extractions which can be expressed as a function of string-->string
   *
   * @param extractionFunction      The function to call when doing the extraction. The function must be able to accept a null input.
   * @param retainMissingValue      Boolean value on if functions which result in `null` should use the original value or should be kept as `null`
   * @param replaceMissingValueWith String value to replace missing values with (instead of `null`)
   * @param injective               If this function always has 1:1 renames and the domain has the same cardinality of the input, this should be true and enables optimized query paths.
   */
  public FunctionalExtraction(
      final Function<String, String> extractionFunction,
      final boolean retainMissingValue,
      final String replaceMissingValueWith,
      final boolean injective
  )
  {
    this.retainMissingValue = retainMissingValue;
    this.replaceMissingValueWith = replaceMissingValueWith;
    Preconditions.checkArgument(
        !(this.retainMissingValue && !(this.replaceMissingValueWith == null)),
        "Cannot specify a [replaceMissingValueWith] and set [retainMissingValue] to true"
    );

    // If missing values are to be retained, we have a slightly different code path
    // This is intended to have the absolutely fastest code path possible and not have any extra logic in the function
    if (this.retainMissingValue) {

      this.extractionFunction = new Function<>()
      {
        @Nullable
        @Override
        public String apply(@Nullable String dimValue)
        {
          final String retval = extractionFunction.apply(dimValue);
          return retval == null ? dimValue : retval;
        }
      };
    } else {
      this.extractionFunction = new Function<>()
      {
        @Nullable
        @Override
        public String apply(@Nullable String dimValue)
        {
          final String retval = extractionFunction.apply(dimValue);
          return retval == null
                 ? FunctionalExtraction.this.replaceMissingValueWith
                 : retval;
        }
      };
    }
    this.extractionType = injective
                          ? ExtractionType.ONE_TO_ONE
                          : ExtractionType.MANY_TO_ONE;
  }

  public boolean isRetainMissingValue()
  {
    return retainMissingValue;
  }

  public String getReplaceMissingValueWith()
  {
    return replaceMissingValueWith;
  }

  public boolean isInjective()
  {
    return ExtractionType.ONE_TO_ONE.equals(getExtractionType());
  }

  @Override
  @Nullable
  public String apply(@Nullable String value)
  {
    return extractionFunction.apply(value);
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return extractionType;
  }
}
