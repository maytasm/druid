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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.sql.calcite.run.EngineFeature;

/**
 * Container class for {@link #validateLogicalQueryForDruid}.
 */
public class QueryValidations
{
  /**
   * Validate a {@link RelNode} prior to attempting to convert it to a Druid query. Useful for generating nice error
   * messages for things we know we definitely don't support.
   */
  public static void validateLogicalQueryForDruid(
      final PlannerContext plannerContext,
      final RelNode relNode
  ) throws ValidationException
  {
    validateNoIllegalRightyJoins(plannerContext, relNode);
  }

  /**
   * Validate that {@link RelNode} does not contain RIGHT or FULL broadcast join if the current engine lacks the
   * feature {@link EngineFeature#ALLOW_BROADCAST_RIGHTY_JOIN}.
   */
  private static void validateNoIllegalRightyJoins(
      final PlannerContext plannerContext,
      final RelNode relNode
  ) throws ValidationException
  {
    if (plannerContext.getJoinAlgorithm() == JoinAlgorithm.BROADCAST
        && !plannerContext.featureAvailable(EngineFeature.ALLOW_BROADCAST_RIGHTY_JOIN)) {
      class FindRightyJoin extends RelShuttleImpl
      {
        private Join found = null;

        @Override
        public RelNode visit(LogicalJoin join)
        {
          if (join.getJoinType().generatesNullsOnLeft()) {
            found = join;
          }

          return visitChildren(join);
        }
      }

      final FindRightyJoin shuttle = new FindRightyJoin();
      relNode.accept(shuttle);
      if (shuttle.found != null) {
        throw new ValidationException(
            StringUtils.format(
                "%s JOIN is not supported by engine[%s] with %s[%s]. Try %s[%s].",
                shuttle.found.getJoinType(),
                plannerContext.getEngine().name(),
                PlannerContext.CTX_SQL_JOIN_ALGORITHM,
                plannerContext.getJoinAlgorithm(),
                PlannerContext.CTX_SQL_JOIN_ALGORITHM,
                JoinAlgorithm.SORT_MERGE.toString()
            )
        );
      }
    }
  }
}
