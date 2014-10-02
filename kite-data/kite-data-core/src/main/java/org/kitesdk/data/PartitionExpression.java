/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data;

import org.kitesdk.data.spi.partition.PartitionFunctions;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;
import org.kitesdk.data.spi.FieldPartitioner;

/**
 * Internal utility class for persisting partition strategies,
 * not a part of the public API.
 */
class PartitionExpression {

  private JexlEngine engine;
  private Expression expression;
  private boolean isStrict;

  public PartitionExpression(String expression, boolean isStrict) {
    this.engine = new JexlEngine();
    Map<String, Object> fns = new HashMap<String, Object>();
    fns.put(null, PartitionFunctions.class);
    this.engine.setFunctions(fns);
    this.engine.setStrict(true);
    this.engine.setSilent(false);
    this.engine.setCache(10);
    this.expression = engine.createExpression(expression);
    this.isStrict = isStrict;
  }

  public PartitionStrategy evaluate() {
    Object object = expression.evaluate(null);
    if (object instanceof FieldPartitioner) {
      return new PartitionStrategy(
          Lists.newArrayList((FieldPartitioner) object));
    } else if (object instanceof Object[]) {
      /*
       * JEXL doesn't recognize that [hash(...), range(...)] is an array of
       * FieldPartitioner. Instead, it thinks it's Object[].
       */
      List<FieldPartitioner> partitioners = Lists.newArrayList();
      for (Object o : ((Object[]) object)) {
        partitioners.add((FieldPartitioner) o);
      }
      return new PartitionStrategy(partitioners);
    } else {
      throw new IllegalArgumentException(
          "Partition expression did not produce FieldPartitioner result (or array) for value:"
              + object);
    }
  }

  /**
   * Convert a PartitionStrategy into a serialized expression. This can be used
   * to set a PartitionStrategy in an Avro property if the PartitionStrategy is
   * passed as an object.
   */
  public static String toExpression(PartitionStrategy partitionStrategy) {
    List<FieldPartitioner> fieldPartitioners = partitionStrategy
        .getFieldPartitioners();
    if (fieldPartitioners.size() == 1) {
      return PartitionFunctions.toExpression(fieldPartitioners.get(0));
    }
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (FieldPartitioner fieldPartitioner : fieldPartitioners) {
      if (sb.length() > 1) {
        sb.append(", ");
      }
      sb.append(PartitionFunctions.toExpression(fieldPartitioner));
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("expression", expression)
        .add("isStrict", isStrict).add("engine", engine).toString();
  }

}
