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

package org.apache.druid.server;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.context.ResponseContext;

public class QueryResponse<T>
{
  private final Sequence<T> results;
  private final ResponseContext responseContext;

  public QueryResponse(final Sequence<T> results, final ResponseContext responseContext)
  {
    this.results = results;
    this.responseContext = responseContext;
  }

  public static <T> QueryResponse<T> withEmptyContext(Sequence<T> results)
  {
    return new QueryResponse<>(results, ResponseContext.createEmpty());
  }

  public Sequence<T> getResults()
  {
    return results;
  }

  public ResponseContext getResponseContext()
  {
    return responseContext;
  }
}
