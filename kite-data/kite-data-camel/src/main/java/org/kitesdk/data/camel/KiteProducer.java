/*
 * Copyright 2015 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.camel;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.camel.CamelException;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.apache.camel.util.ObjectHelper;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class KiteProducer extends DefaultProducer {

    private DatasetDescriptor.Builder builder;

    private DatasetDescriptor descriptor;

    private Dataset<Object> dataset;

    private DatasetWriter<Object> writer;

    private boolean lazy = false;

    public KiteProducer(Endpoint endpoint) {
        super(endpoint);
    }

    @Override
    public void doStart() throws Exception {
        try {
            KiteEndpoint ep = (KiteEndpoint) getEndpoint();

            if (ep.isAppend()) {
                dataset = Datasets.load(ep.getUri(), Object.class);
                descriptor = dataset.getDescriptor();
                writer = dataset.newWriter();
            } else {

                String schema = ep.getSchema();

                if (schema == null)
                    throw new CamelException("the schema must be defined");

                String ps = ep.getPartitionStrategy();

                builder = new DatasetDescriptor.Builder().compressionType(ep.getCompressionType()).format(ep.getFormat());
                if (ps != null) {
                    if (ps.startsWith("literal:"))
                        builder.partitionStrategyLiteral(ps.split("literal:")[1]);
                    else
                        builder.partitionStrategyUri(ps);
                }
                if (schema.equals("lazy")) {
                    lazy = true;
                } else if (schema.startsWith("class:")) {
                    String className = schema.split("class:")[1];
                    Class<?> klass = ObjectHelper.loadClass(className);
                    descriptor = builder.schema(klass).build();
                    dataset = Datasets.create(ep.getUri(), descriptor, Object.class);
                    writer = dataset.newWriter();
                } else {
                    descriptor = builder.schemaUri(ep.getSchema()).build();
                    dataset = Datasets.create(ep.getUri(), descriptor, Object.class);
                    writer = dataset.newWriter();
                }
            }
        } catch (IOException e) {
            throw new CamelException(e);
        }
    }

    @Override
    public void doStop() {
        writer.close();
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        try {
            Object record = exchange.getIn().getBody();
            if (lazy && descriptor == null) {
                KiteEndpoint ep = (KiteEndpoint) getEndpoint();
                if (ep.isAppend())
                    throw new CamelException("lazy is not support in append mode");
                Class<?> instanceClass = record.getClass();
                if (GenericContainer.class.isAssignableFrom(instanceClass)) {
                    Method method = instanceClass.getMethod("getSchema");
                    Schema schema = (Schema) method.invoke(record);
                    descriptor = builder.schema(schema).build();
                    dataset = Datasets.create(ep.getUri(), descriptor, Object.class);
                    writer = dataset.newWriter();
                } else {
                    throw new CamelException("Class " + instanceClass + " must be instanceof " + GenericContainer.class.getName());
                }
            }
            if (writer == null)
                throw new CamelException("something went wrong");
            else
                writer.write(record);
        } catch (InvocationTargetException e) {
            throw new CamelException(e);
        } catch (NoSuchMethodException e) {
            throw new CamelException(e);
        } catch (IllegalAccessException e) {
            throw new CamelException(e);
        }
    }
}
