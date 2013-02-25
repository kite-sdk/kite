package com.cloudera.data.partition;

import java.beans.PropertyDescriptor;

public abstract class Partition {

  private String name;
  private int cardinality;
  private Partition subpartition;

  protected Partition(String name, int cardinality, Partition subpartition) {
    this.name = name;
    this.cardinality = cardinality;
    this.subpartition = subpartition;
  }

  public String getName() {
    return name;
  }

  public int getCardinality() {
    return cardinality;
  }

  public int getAggregateCardinality() {
    return cardinality * (getSubpartition() == null ? 1 : getSubpartition().getCardinality());
  }

  public Partition getSubpartition() {
    return subpartition;
  }

  public String getPath(Object entity) throws Exception {
    Object value = new PropertyDescriptor(getName(), entity.getClass()).getReadMethod().invoke(entity); // TODO: cache property descriptors
    String eval = getName() + "=" + evaluate(value);
    return getSubpartition() == null ? eval : eval + "/" + getSubpartition().getPath(entity);
  }

  protected abstract Object evaluate(Object value) throws Exception;
}
