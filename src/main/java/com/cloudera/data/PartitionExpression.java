package com.cloudera.data;

import com.cloudera.data.partition.HashPartitionStrategy;
import com.cloudera.data.partition.IdentityPartitionStrategy;
import com.cloudera.data.partition.IntRangePartitionStrategy;
import com.cloudera.data.partition.PartitionFunctions;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

public class PartitionExpression {

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
    if (object instanceof PartitionStrategy) {
      return (PartitionStrategy) object;
    } else if (object instanceof PartitionStrategy[]) {
      PartitionStrategy[] strategies = (PartitionStrategy[]) object;
      for (int i = strategies.length - 2; i >= 0; i--) {
        strategies[i].setPartitionStrategy(strategies[i + 1]);
      }
      return strategies[0];
    } else {
      throw new IllegalArgumentException("Partition expression did not produce PartitionStrategy result (or array) for value:" + object);
    }
  }

  /**
   * Convert a PartitionStrategy into a serialized expression. This can be used to set a PartitionStrategy
   * in an Avro property if the PartitionStrategy is passed as an object.
   */
  public static String toExpression(PartitionStrategy partitionStrategy) {
    if (!partitionStrategy.isPartitioned()) {
      return toAtomicExpression(partitionStrategy);
    }
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    PartitionStrategy part = partitionStrategy;
    do {
      if (sb.length() > 1) {
        sb.append(", ");
      }
      sb.append(toAtomicExpression(part));
      part = part.getPartitionStrategy();
    } while (part != null);
    sb.append("]");
    return sb.toString();
  }

  private static String toAtomicExpression(PartitionStrategy partitionStrategy) {
    // TODO: add other strategies
    if (partitionStrategy instanceof HashPartitionStrategy) {
      return String.format("hash(\"%s\", %s)", partitionStrategy.getName(), partitionStrategy.getCardinality());
    } else if (partitionStrategy instanceof IdentityPartitionStrategy) {
      return String.format("identity(\"%s\", %s)", partitionStrategy.getName(), partitionStrategy.getCardinality());
    }
    throw new IllegalArgumentException("Unrecognized PartitionStrategy: " + partitionStrategy);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("expression", expression)
        .add("isStrict", isStrict).add("engine", engine).toString();
  }

}
