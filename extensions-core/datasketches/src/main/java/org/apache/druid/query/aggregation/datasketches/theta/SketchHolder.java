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

package org.apache.druid.query.aggregation.datasketches.theta;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.AnotB;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.data.SafeWritableMemory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Comparator;

/**
 */
public class SketchHolder
{
  public static final SketchHolder EMPTY = SketchHolder.of(
      Sketches.updateSketchBuilder()
              .build()
              .compact(true, null)
  );

  public static final Comparator<Object> COMPARATOR = Ordering.from(
      new Comparator<>()
      {
        @Override
        public int compare(Object o1, Object o2)
        {
          SketchHolder h1 = (SketchHolder) o1;
          SketchHolder h2 = (SketchHolder) o2;

          if (h1.obj instanceof Sketch || h1.obj instanceof Union) {
            if (h2.obj instanceof Sketch || h2.obj instanceof Union) {
              return SKETCH_COMPARATOR.compare(h1.getSketch(), h2.getSketch());
            } else {
              return -1;
            }
          }

          if (h1.obj instanceof Memory) {
            if (h2.obj instanceof Memory) {
              return MEMORY_COMPARATOR.compare((Memory) h1.obj, (Memory) h2.obj);
            } else {
              return 1;
            }
          }

          throw new IAE("Unknwon types [%s] and [%s]", h1.obj.getClass().getName(), h2.obj.getClass().getName());
        }
      }
  ).nullsFirst();

  private static final Comparator<Sketch> SKETCH_COMPARATOR = new Comparator<>()
  {
    @Override
    public int compare(Sketch o1, Sketch o2)
    {
      return Doubles.compare(o1.getEstimate(), o2.getEstimate());
    }
  };

  private static final Comparator<Memory> MEMORY_COMPARATOR = new Comparator<>()
  {
    @SuppressWarnings("SubtractionInCompareTo")
    @Override
    public int compare(Memory o1, Memory o2)
    {
      // We have two Ordered Compact sketches, so just compare their last entry if they have the size.
      // This is to produce a deterministic ordering, though it might not match the actual estimate
      // ordering, but that's ok because this comparator is only used by GenericIndexed
      int retVal = Longs.compare(o1.getCapacity(), o2.getCapacity());
      if (retVal == 0) {
        retVal = Longs.compare(o1.getLong(o2.getCapacity() - 8), o2.getLong(o2.getCapacity() - 8));
      }

      return retVal;
    }
  };


  private final Object obj;

  @Nullable
  private volatile Double cachedEstimate = null;
  @Nullable
  private volatile Sketch cachedSketch = null;

  private SketchHolder(Object obj)
  {
    Preconditions.checkArgument(
        obj instanceof Sketch || obj instanceof Union || obj instanceof Memory,
        "unknown sketch representation type [%s]", obj.getClass().getName()
    );
    this.obj = obj;
  }

  public static SketchHolder of(Object obj)
  {
    return new SketchHolder(obj);
  }

  public void updateUnion(Union union)
  {
    if (obj instanceof Memory) {
      union.union((Memory) obj);
    } else {
      union.union(getSketch());
    }
  }

  public Sketch getSketch()
  {
    if (cachedSketch != null) {
      return cachedSketch;
    }

    if (obj instanceof Sketch) {
      cachedSketch = (Sketch) obj;
    } else if (obj instanceof Union) {
      cachedSketch = ((Union) obj).getResult();
    } else if (obj instanceof Memory) {
      cachedSketch = deserializeFromMemory((Memory) obj);
    } else {
      throw new ISE("Can't get sketch from object of type [%s]", obj.getClass().getName());
    }
    return cachedSketch;
  }

  public double getEstimate()
  {
    if (cachedEstimate == null) {
      cachedEstimate = getSketch().getEstimate();
    }
    return cachedEstimate.doubleValue();
  }

  public SketchEstimateWithErrorBounds getEstimateWithErrorBounds(int errorBoundsStdDev)
  {
    Sketch sketch = getSketch();
    SketchEstimateWithErrorBounds result = new SketchEstimateWithErrorBounds(
        getEstimate(),
        sketch.getUpperBound(errorBoundsStdDev),
        sketch.getLowerBound(errorBoundsStdDev),
        errorBoundsStdDev);
    return result;
  }

  public static SketchHolder combine(Object o1, Object o2, int nomEntries)
  {
    SketchHolder holder1 = (SketchHolder) o1;
    SketchHolder holder2 = (SketchHolder) o2;

    if (holder1.obj instanceof Union) {
      Union union = (Union) holder1.obj;
      holder2.updateUnion(union);
      holder1.invalidateCache();
      return holder1;
    } else if (holder2.obj instanceof Union) {
      Union union = (Union) holder2.obj;
      holder1.updateUnion(union);
      holder2.invalidateCache();
      return holder2;
    } else {
      Union union = (Union) SetOperation.builder().setNominalEntries(nomEntries).build(Family.UNION);
      holder1.updateUnion(union);
      holder2.updateUnion(union);
      return SketchHolder.of(union);
    }
  }

  void invalidateCache()
  {
    cachedEstimate = null;
    cachedSketch = null;
  }

  public static SketchHolder deserialize(Object serializedSketch)
  {
    if (serializedSketch instanceof String) {
      return SketchHolder.of(deserializeFromBase64EncodedString((String) serializedSketch));
    } else if (serializedSketch instanceof byte[]) {
      return SketchHolder.of(deserializeFromByteArray((byte[]) serializedSketch));
    } else if (serializedSketch instanceof SketchHolder) {
      return (SketchHolder) serializedSketch;
    } else if (serializedSketch instanceof Sketch
               || serializedSketch instanceof Union
               || serializedSketch instanceof Memory) {
      return SketchHolder.of(serializedSketch);
    }

    throw new ISE(
        "Object is not of a type[%s] that can be deserialized to sketch.",
        serializedSketch.getClass()
    );
  }

  public static SketchHolder deserializeSafe(Object serializedSketch)
  {
    if (serializedSketch instanceof String) {
      return SketchHolder.of(deserializeFromBase64EncodedStringSafe((String) serializedSketch));
    } else if (serializedSketch instanceof byte[]) {
      return SketchHolder.of(deserializeFromByteArraySafe((byte[]) serializedSketch));
    }

    return deserialize(serializedSketch);
  }

  private static Sketch deserializeFromBase64EncodedString(String str)
  {
    return deserializeFromByteArray(StringUtils.decodeBase64(StringUtils.toUtf8(str)));
  }

  private static Sketch deserializeFromByteArray(byte[] data)
  {
    return deserializeFromMemory(Memory.wrap(data));
  }

  private static Sketch deserializeFromBase64EncodedStringSafe(String str)
  {
    return deserializeFromByteArraySafe(StringUtils.decodeBase64(StringUtils.toUtf8(str)));
  }

  private static Sketch deserializeFromByteArraySafe(byte[] data)
  {
    return deserializeFromMemory(SafeWritableMemory.wrap(data));
  }

  private static Sketch deserializeFromMemory(Memory mem)
  {
    if (Sketch.getSerializationVersion(mem) < 3) {
      return Sketches.heapifySketch(mem);
    } else {
      return Sketches.wrapSketch(mem);
    }
  }

  public enum Func
  {
    UNION,
    INTERSECT,
    NOT
  }

  public static SketchHolder sketchSetOperation(Func func, int sketchSize, Object... holders)
  {
    //in the code below, I am returning SetOp.getResult(false, null)
    //"false" gets us an unordered sketch which is faster to build
    //"true" returns an ordered sketch but slower to compute. advantage of ordered sketch
    //is that they are faster to "union" later but given that this method is used in
    //the final stages of query processing, ordered sketch would be of no use.
    switch (func) {
      case UNION:
        Union union = (Union) SetOperation.builder().setNominalEntries(sketchSize).build(Family.UNION);
        for (Object o : holders) {
          ((SketchHolder) o).updateUnion(union);
        }
        return SketchHolder.of(union);
      case INTERSECT:
        Intersection intersection = (Intersection) SetOperation.builder().setNominalEntries(sketchSize).build(Family.INTERSECTION);
        for (Object o : holders) {
          intersection.intersect(((SketchHolder) o).getSketch());
        }
        return SketchHolder.of(intersection.getResult(false, null));
      case NOT:
        if (holders.length < 1) {
          throw new IllegalArgumentException("A-Not-B requires at least 1 sketch");
        }

        if (holders.length == 1) {
          return (SketchHolder) holders[0];
        }

        Sketch result = ((SketchHolder) holders[0]).getSketch();
        for (int i = 1; i < holders.length; i++) {
          AnotB anotb = (AnotB) SetOperation.builder().setNominalEntries(sketchSize).build(Family.A_NOT_B);
          result = anotb.aNotB(result, ((SketchHolder) holders[i]).getSketch());
        }
        return SketchHolder.of(result);
      default:
        throw new IllegalArgumentException("Unknown sketch operation " + func);
    }
  }

  /**
   *  Ideally make use of Sketch's equals and hashCode methods but which are not value based implementations.
   *  And yet need value based equals and hashCode implementations for SketchHolder. 
   *  Hence using Arrays.equals() and Arrays.hashCode().
   */
  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return Arrays.equals(this.getSketch().toByteArray(), ((SketchHolder) o).getSketch().toByteArray());
  }
  
  @Override
  public int hashCode()
  {
    return 31 * Arrays.hashCode(this.getSketch().toByteArray());
  }
}
