package com.cloudera.data;

import java.io.IOException;

public interface MetadataProvider {

  DatasetDescriptor load(String name) throws IOException;

  void save(String name, DatasetDescriptor descriptor) throws IOException;

}
