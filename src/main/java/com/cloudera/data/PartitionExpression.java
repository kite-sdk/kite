package com.cloudera.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;

import com.cloudera.data.partition.HashFieldPartitioner;
import com.cloudera.data.partition.IdentityFieldPartitioner;
import com.cloudera.data.partition.PartitionFunctions;
import com.google.common.base.Objects;

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
    if (object instanceof FieldPartitioner) {
      return new PartitionStrategy((FieldPartitioner) object);
    } else if (object instanceof Object[]) {
      /*
       * JEXL doesn't recognize that [hash(...), range(...)] is an array of
       * FieldPartitioner. Instead, it thinks it's Object[].
       */
      return new PartitionStrategy((FieldPartitioner[]) object);
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
      return toExpression(fieldPartitioners.get(0));
    }
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (FieldPartitioner fieldPartitioner : fieldPartitioners) {
      if (sb.length() > 1) {
        sb.append(", ");
      }
      sb.append(toExpression(fieldPartitioner));
    }
    sb.append("]");
    return sb.toString();
  }

  private static String toExpression(FieldPartitioner fieldPartitioner) {
    // TODO: add other strategies
    if (fieldPartitioner instanceof HashFieldPartitioner) {
      return String.format("hash(\"%s\", %s)", fieldPartitioner.getName(),
          fieldPartitioner.getCardinality());
    } else if (fieldPartitioner instanceof IdentityFieldPartitioner) {
      return String.format("identity(\"%s\", %s)", fieldPartitioner.getName(),
          fieldPartitioner.getCardinality());
    }
    throw new IllegalArgumentException("Unrecognized PartitionStrategy: "
        + fieldPartitioner);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("expression", expression)
        .add("isStrict", isStrict).add("engine", engine).toString();
  }

}
