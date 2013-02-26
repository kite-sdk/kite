package com.cloudera.data;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class PartitionExpression {

  private JexlEngine engine;
  private Expression expression;
  private boolean isStrict;

  public PartitionExpression(String expression, boolean isStrict) {
    this.engine = new JexlEngine();
    this.engine.setStrict(true);
    this.engine.setSilent(false);
    this.engine.setCache(10);
    this.expression = engine.createExpression(expression);
    this.isStrict = isStrict;
  }

  public PartitionStrategy evaluate() {
    Object object = expression.evaluate(null);
    Preconditions.checkArgument(object instanceof PartitionStrategy,
        "Partition expression did not produce PartitionStrategy result for value:%s",
        object);
    return (PartitionStrategy) object;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("expression", expression)
        .add("isStrict", isStrict).add("engine", engine).toString();
  }

}
