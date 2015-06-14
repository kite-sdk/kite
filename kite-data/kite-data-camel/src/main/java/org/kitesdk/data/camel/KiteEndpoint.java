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

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;

import java.net.URI;
import java.net.URISyntaxException;

@UriEndpoint(scheme = "kite", title = "Kite", syntax = "kite:dataset", consumerClass = DefaultConsumer.class, producerOnly = true, label = "hadoop")
public class KiteEndpoint extends DefaultEndpoint {

    @UriPath
    @Metadata(required = "true")
    private URI uri;

    @UriParam(name = "schema", defaultValue = "lazy")
    private String schema = "lazy";

    @UriParam(name = "compressionType", defaultValue = "snappy")
    private String compressionType = "snappy";

    @UriParam(name = "format", defaultValue = "avro")
    private String format = "avro";

    @UriParam(name = "partitionStrategy")
    private String partitionStrategy;

    @UriParam(name = "append", defaultValue = "false")
    private boolean append = false;

    protected KiteEndpoint(String endpointUri, String remaining, KiteComponent component) throws URISyntaxException {
        super(endpointUri, component);
        this.uri = new URI(remaining);
    }

    @Override
    public Producer createProducer() throws Exception {
        return new KiteProducer(this);
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        throw new RuntimeCamelException("The Kite Dataset Consumer is not supported");
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setUri(URI uri) {
        this.uri = uri;
    }

    public URI getUri() {
        return this.uri;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(String compressionType) {
        this.compressionType = compressionType;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getPartitionStrategy() {
        return partitionStrategy;
    }

    public void setPartitionStrategy(String partitionStrategy) {
        this.partitionStrategy = partitionStrategy;
    }

    public boolean isAppend() {
        return append;
    }

    public void setAppend(String append) {
        this.append = Boolean.parseBoolean(append);
    }
}
