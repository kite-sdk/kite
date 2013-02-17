package com.cloudera.data;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

public class PartitionExpression {

  private static final Logger logger = LoggerFactory
      .getLogger(PartitionExpression.class);

  private JexlEngine engine;
  private Expression expression;

  public PartitionExpression(String expression) {
    this.engine = new JexlEngine();
    this.engine.setStrict(true);
    this.engine.setSilent(false);
    this.engine.setCache(10);
    this.expression = engine.createExpression(expression);
  }

  public String evaluate(Object record) {
    JexlContext context = new MapContext();

    context.set("record", record);

    //logger.debug("expression:{} context:{}", expression, context);
    Object object = expression.evaluate(context);

    //logger.debug("result:{} type:{}", object, object.getClass());

    StringBuilder builder = new StringBuilder();

    if (object instanceof Object[]) {
      for (Object element : (Object[]) object) {
        if (builder.length() > 0) {
          builder.append("/");
        }

        builder.append(element.toString());
      }
    } else {
      builder.append(object.toString());
    }

    return builder.toString();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("expression", expression)
        .add("engine", engine).toString();
  }

}
