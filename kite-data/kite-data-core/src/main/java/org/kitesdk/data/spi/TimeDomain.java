/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.FieldPartitioner;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.partition.CalendarFieldPartitioner;

@Immutable
public class TimeDomain {
  private static final List<Integer> order = Lists.newArrayList(
      Calendar.YEAR, Calendar.MONTH, Calendar.DAY_OF_MONTH,
      Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND
  );

  private static final
  LoadingCache<Pair<PartitionStrategy, String>, TimeDomain> domains =
      CacheBuilder.newBuilder().build(
          new CacheLoader<Pair<PartitionStrategy, String>, TimeDomain>() {
        @Override
        public TimeDomain load(Pair<PartitionStrategy, String> entry) {
          return new TimeDomain(entry.first(), entry.second());
        }
      });

  public static TimeDomain get(PartitionStrategy strategy, String source) {
    return domains.getUnchecked(Pair.of(strategy, source));
  }

  // the calendar field partitioners from the strategy, in the correct order
  private final List<CalendarFieldPartitioner> partitioners;

  public TimeDomain(PartitionStrategy strategy, String sourceName) {
    Map<Integer, CalendarFieldPartitioner> mapping = Maps.newHashMap();
    for (FieldPartitioner fp : strategy.getFieldPartitioners()) {
      // there may be partitioners for more than one source field
      if (sourceName.equals(fp.getSourceName()) &&
          fp instanceof CalendarFieldPartitioner) {
        mapping.put(
            ((CalendarFieldPartitioner) fp).getCalendarField(),
            (CalendarFieldPartitioner) fp);
      }
    }
    // get the partitioners to check for this strategy
    this.partitioners = Lists.newArrayList();
    for (int field : order) {
      // if there is no partition for the next field, then all are included
      // example: yyyy/mm/dd partitioning accepts when field is hour
      if (!mapping.containsKey(field)) {
        break;
      }
      partitioners.add(mapping.get(field));
    }
  }

  public Predicate<StorageKey> project(Predicate<Long> predicate) {
    if (predicate instanceof Predicates.In) {
      return new TimeSetPredicate((Predicates.In<Long>) predicate);
    } else if (predicate instanceof Range) {
      return new TimeRangePredicate((Range<Long>) predicate);
    } else {
      return null;
    }
  }

  private class TimeSetPredicate implements Predicate<StorageKey> {
    private final Predicates.In<List<Integer>> times;

    private TimeSetPredicate(Predicates.In<Long> times) {
      this.times = times.transform(new Function<Long, List<Integer>>() {
        @Override
        public List<Integer> apply(@Nullable Long timestamp) {
          List<Integer> time = Lists
              .newArrayListWithExpectedSize(partitioners.size());
          for (CalendarFieldPartitioner fp : partitioners) {
            time.add(fp.apply(timestamp));
          }
          return time;
        }
      });
    }

    @Override
    public boolean apply(@Nullable StorageKey key) {
      List<Integer> time = Lists
          .newArrayListWithExpectedSize(partitioners.size());
      for (CalendarFieldPartitioner fp : partitioners) {
        time.add((Integer) key.get(fp.getName()));
      }
      return times.apply(time);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("in", times).toString();
    }
  }

  private class TimeRangePredicate implements Predicate<StorageKey> {
    private final boolean hasLower;
    private final boolean hasUpper;
    private final long lower;
    private final long upper;

    private TimeRangePredicate(Range<Long> timeRange) {
      // adjust to a closed range to avoid catching extra keys
      this.hasLower = timeRange.hasLowerBound();
      if (hasLower) {
        this.lower = timeRange.lowerEndpoint() +
           (BoundType.CLOSED == timeRange.lowerBoundType() ? 0 : 1);
      } else {
        this.lower = 0;
      }
      this.hasUpper = timeRange.hasUpperBound();
      if (hasUpper) {
        this.upper = timeRange.upperEndpoint() -
            (BoundType.CLOSED == timeRange.upperBoundType() ? 0 : 1);
      } else {
        this.upper = 0;
      }
    }

    @Override
    public boolean apply(@Nullable StorageKey key) {
      if (key == null) {
        return false;
      }
      boolean returnVal = true; // no bounds => accept
      if (hasLower) {
        returnVal = checkLower(key, lower);
      }
      if (returnVal && hasUpper) {
        returnVal = checkUpper(key, upper);
      }
      return returnVal;
    }

    private boolean checkLower(StorageKey key, long timestamp) {
      for (CalendarFieldPartitioner calField : partitioners) {
        int value = (Integer) key.get(calField.getName());
        int lower = calField.apply(timestamp);
        if (value < lower) {
          // strictly within range, so all other levels must be
          // example: 2013-4-10 to 2013-10-4 => 4 < month < 10 => accept
          return false;
        } else if (value > lower) {
          // falls out of the range at this level
          return true;
        }
        // value was equal to one endpoint, continue checking
      }
      // each position was satisfied, so the key matches
      return true;
    }

    private boolean checkUpper(StorageKey key, long timestamp) {
      // same as checkLower, see comments there
      for (CalendarFieldPartitioner calField : partitioners) {
        int value = (Integer) key.get(calField.getName());
        int upper = calField.apply(timestamp);
        if (value > upper) {
          return false;
        } else if (value < upper) {
          return true;
        }
      }
      return true;
    }

    @Override
    public String toString() {
      Objects.ToStringHelper helper = Objects.toStringHelper(this);
      if (hasLower) {
        helper.add("lower", lower);
      }
      if (hasUpper) {
        helper.add("upper", upper);
      }
      return helper.toString();
    }
  }

  @SuppressWarnings("unchecked")
  Iterator<MarkerRange.Builder> addStackedIterator(
      Predicate<Long> timePredicate,
      Iterator<MarkerRange.Builder> inner) {
    if (timePredicate instanceof Predicates.In) {
      // normal group handling is sufficient for a set of specific times
      // instantiate directly because the add method projects the predicate
      return new KeyRangeIterable.SetGroupIterator(
          (Predicates.In) timePredicate, (List) partitioners, inner);
    } else if (timePredicate instanceof Range) {
      return new TimeRangeIterator(
          (Range<Long>) timePredicate, partitioners, inner);
    }
    return null;
  }

  private static class TimeRangeIterator extends
      KeyRangeIterable.StackedIterator<Range<Long>, MarkerRange.Builder> {
    private final List<CalendarFieldPartitioner> fields;
    private TimeRangeIterator(Range<Long> timeRange, List<CalendarFieldPartitioner> fps,
                              Iterator<MarkerRange.Builder> inner) {
      this.fields = fps;
      setItem(timeRange);
      setInner(inner);
    }

    @Override
    public MarkerRange.Builder update(
        MarkerRange.Builder current, Range<Long> range) {
      // FIXME: this assumes all of the partition fields are in order
      // This should identify out-of-order fields and alter the range
      for (CalendarFieldPartitioner cfp : fields) {
        boolean hasLower = range.hasLowerBound();
        boolean hasUpper = range.hasUpperBound();
        if (hasLower) {
          current.addToStart(cfp.getName(), cfp.apply(range.lowerEndpoint()));
        }
        if (hasUpper) {
          current.addToEnd(cfp.getName(), cfp.apply(range.upperEndpoint()));
        }
      }
      return current;
    }
  }
}
