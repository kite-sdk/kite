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
import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.partition.CalendarFieldPartitioner;
import org.kitesdk.data.spi.predicates.Exists;
import org.kitesdk.data.spi.predicates.In;
import org.kitesdk.data.spi.predicates.Predicates;
import org.kitesdk.data.spi.predicates.Range;
import org.kitesdk.data.spi.predicates.Ranges;

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
    for (FieldPartitioner fp : Accessor.getDefault().getFieldPartitioners(strategy)) {
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
      if (mapping.containsKey(field)) {
        partitioners.add(mapping.get(field));
      } else if (!partitioners.isEmpty()) {
        break;
      }
    }
  }

  public Predicate<Marker> project(Predicate<Long> predicate) {
    if (predicate instanceof In) {
      return new TimeSetPredicate((In<Long>) predicate);
    } else if (predicate instanceof Range) {
      return new TimeRangePredicate((Range<Long>) predicate);
    } else {
      return null;
    }
  }

  public Predicate<Marker> projectStrict(Predicate<Long> predicate) {
    if (predicate instanceof Exists) {
      return Predicates.exists();
    } else if (predicate instanceof In) {
      return null;
    } else if (predicate instanceof Range) {
      return new TimeRangeStrictPredicate((Range<Long>) predicate);
    } else {
      return null;
    }
  }

  private class TimeSetPredicate implements Predicate<Marker> {
    private final In<List<Integer>> times;

    private TimeSetPredicate(In<Long> times) {
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
    public boolean apply(@Nullable Marker key) {
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

  /**
   * Predicate that accepts a {@link StorageKey} if could include an entity
   * that would be accepted by the original time range.
   */
  private class TimeRangePredicate extends TimeRangePredicateImpl {
    private TimeRangePredicate(Range<Long> timeRange) {
      // adjust the range end-points if exclusive to avoid extra partitions
      super(timeRange, true /* accept end-points */ );
    }
  }

  /**
   * Predicate that accepts a {@link StorageKey} only if entities it includes
   * must be accepted by the original time range.
   */
  private class TimeRangeStrictPredicate extends TimeRangePredicateImpl {
    private TimeRangeStrictPredicate(Range<Long> timeRange) {
      super(timeRange, false /* exclude end-points */ );
    }
  }

  /**
   * A common implementation class for time-based range predicates.
   */
  private class TimeRangePredicateImpl implements Predicate<Marker> {
    private final Range<Long> range;
    private final String[] names;
    private final int[] lower;
    private final int[] upper;
    private final boolean acceptEqual;

    private TimeRangePredicateImpl(Range<Long> timeRange, boolean acceptEqual) {
      this.range = Ranges.adjustClosed(timeRange, DiscreteDomains.longs());
      this.acceptEqual = acceptEqual;

      int length = partitioners.size();
      this.names = new String[length];
      for (int i = 0; i < length; i += 1) {
        names[i] = partitioners.get(i).getName();
      }

      if (range.hasLowerBound()) {
        long start = range.lowerEndpoint() - (acceptEqual ? 0 : 1);
        this.lower = new int[length];
        for (int i = 0; i < length; i += 1) {
          lower[i] = partitioners.get(i).apply(start);
        }
      } else {
        this.lower = new int[0];
      }
      if (range.hasUpperBound()) {
        long stop = range.upperEndpoint() + (acceptEqual ? 0 : 1);
        this.upper = new int[length];
        for (int i = 0; i < length; i += 1) {
          upper[i] = partitioners.get(i).apply(stop);
        }
      } else {
        this.upper = new int[0];
      }
    }

    @Override
    public boolean apply(@Nullable Marker key) {
      if (key == null) {
        return false;
      }
      boolean returnVal = true; // no bounds => accept
      if (lower.length > 0) {
        returnVal = checkLower(key);
      }
      if (returnVal && upper.length > 0) {
        returnVal = checkUpper(key);
      }
      return returnVal;
    }

    private boolean checkLower(Marker key) {
      for (int i = 0; i < names.length; i += 1) {
        int value = (Integer) key.get(names[i]);
        if (value < lower[i]) {
          // strictly within range, so all other levels must be
          // example: 2013-4-10 to 2013-10-4 => 4 < month < 10 => accept
          return false;
        } else if (value > lower[i]) {
          // falls out of the range at this level
          return true;
        }
        // value was equal to one endpoint, continue checking
      }
      // each position was satisfied, defer to acceptEqual
      return acceptEqual;
    }

    private boolean checkUpper(Marker key) {
      // same as checkLower, see comments there
      for (int i = 0; i < names.length; i += 1) {
        int value = (Integer) key.get(names[i]);
        if (value > upper[i]) {
          return false;
        } else if (value < upper[i]) {
          return true;
        }
      }
      return acceptEqual;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || !(obj instanceof TimeRangePredicateImpl)) {
        return false;
      }
      TimeRangePredicateImpl that = (TimeRangePredicateImpl) obj;
      if (!range.equals(that.range)) {
        return false;
      }
      // both permissive or both strict
      if (this.acceptEqual == that.acceptEqual) {
        return true;
      }
      // one is strict and the other permissive. the only time the two agree is
      // when the range aligns with a boundary in the partitions. we can detect
      // a boundary when the values differ for the same range.
      if (lower.length > 0) {
        boolean differ = false;
        for (int i = 0; i < lower.length; i += 1) {
          if (lower[i] != that.lower[i]) {
            differ = true;
            break;
          }
        }
        if (!differ) {
          return false;
        }
      }
      if (upper.length > 0) {
        boolean differ = false;
        for (int i = 0; i < upper.length; i += 1) {
          if (upper[i] != that.upper[i]) {
            differ = true;
            break;
          }
        }
        if (!differ) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(range, acceptEqual);
    }

    @Override
    public String toString() {
      Objects.ToStringHelper helper = Objects.toStringHelper(this);
      if (lower.length > 0) {
        helper.add("lower", Arrays.toString(lower));
      }
      if (upper.length > 0) {
        helper.add("upper", Arrays.toString(upper));
      }
      return helper.toString();
    }
  }

  @SuppressWarnings("unchecked")
  Iterator<MarkerRange.Builder> addStackedIterator(
      Predicate<Long> timePredicate,
      Iterator<MarkerRange.Builder> inner) {
    if (timePredicate instanceof In) {
      // normal group handling is sufficient for a set of specific times
      // instantiate directly because the add method projects the predicate
      return new KeyRangeIterable.SetGroupIterator(
          (In) timePredicate, (List) partitioners, inner);
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
