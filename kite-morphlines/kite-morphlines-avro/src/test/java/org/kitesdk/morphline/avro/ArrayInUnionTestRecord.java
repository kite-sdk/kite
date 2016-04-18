/*
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
package org.kitesdk.morphline.avro;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class ArrayInUnionTestRecord extends org.apache.avro.specific.SpecificRecordBase
        implements org.apache.avro.specific.SpecificRecord {
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"ArrayInUnionTestRecord\",\"namespace\":\"org.kitesdk.morphline.avro\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}},{\"name\":\"itemsInUnion\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}],\"default\":null}]}");

    public static org.apache.avro.Schema getClassSchema() {
        return SCHEMA$;
    }

    @Deprecated
    public java.util.List<java.lang.String> items;
    @Deprecated
    public java.util.List<java.lang.String> itemsInUnion;

    /**
     * Default constructor. Note that this does not initialize fields
     * to their default values from the schema. If that is desired then
     * one should use <code>newBuilder()</code>.
     */
    public ArrayInUnionTestRecord() {
    }

    /**
     * All-args constructor.
     */
    public ArrayInUnionTestRecord(final java.util.List<java.lang.String> items,
            final java.util.List<java.lang.String> itemsInUnion) {
        this.items = items;
        this.itemsInUnion = itemsInUnion;
    }

    @Override
    public org.apache.avro.Schema getSchema() {
        return SCHEMA$;
    }

    // Used by DatumWriter. Applications should not call.
    @Override
    public java.lang.Object get(final int field$) {
        switch (field$) {
        case 0:
            return items;
        case 1:
            return itemsInUnion;
        default:
            throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    // Used by DatumReader. Applications should not call.
    @Override
    @SuppressWarnings(value = "unchecked")
    public void put(final int field$, final java.lang.Object value$) {
        switch (field$) {
        case 0:
            items = (java.util.List<java.lang.String>) value$;
            break;
        case 1:
            itemsInUnion = (java.util.List<java.lang.String>) value$;
            break;
        default:
            throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    /**
     * Gets the value of the 'items' field.
     */
    public java.util.List<java.lang.String> getItems() {
        return items;
    }

    /**
     * Sets the value of the 'items' field.
     * 
     * @param value
     *            the value to set.
     */
    public void setItems(final java.util.List<java.lang.String> value) {
        this.items = value;
    }

    /**
     * Gets the value of the 'itemsInUnion' field.
     */
    public java.util.List<java.lang.String> getItemsInUnion() {
        return itemsInUnion;
    }

    /**
     * Sets the value of the 'itemsInUnion' field.
     * 
     * @param value
     *            the value to set.
     */
    public void setItemsInUnion(final java.util.List<java.lang.String> value) {
        this.itemsInUnion = value;
    }

    /**
     * Creates a new ArrayInUnionTestRecord RecordBuilder.
     * 
     * @return A new ArrayInUnionTestRecord RecordBuilder
     */
    public static org.kitesdk.morphline.avro.ArrayInUnionTestRecord.Builder newBuilder() {
        return new org.kitesdk.morphline.avro.ArrayInUnionTestRecord.Builder();
    }

    /**
     * Creates a new ArrayInUnionTestRecord RecordBuilder by copying an existing Builder.
     * 
     * @param other
     *            The existing builder to copy.
     * @return A new ArrayInUnionTestRecord RecordBuilder
     */
    public static org.kitesdk.morphline.avro.ArrayInUnionTestRecord.Builder newBuilder(
            final org.kitesdk.morphline.avro.ArrayInUnionTestRecord.Builder other) {
        return new org.kitesdk.morphline.avro.ArrayInUnionTestRecord.Builder(other);
    }

    /**
     * Creates a new ArrayInUnionTestRecord RecordBuilder by copying an existing ArrayInUnionTestRecord instance.
     * 
     * @param other
     *            The existing instance to copy.
     * @return A new ArrayInUnionTestRecord RecordBuilder
     */
    public static org.kitesdk.morphline.avro.ArrayInUnionTestRecord.Builder newBuilder(
            final org.kitesdk.morphline.avro.ArrayInUnionTestRecord other) {
        return new org.kitesdk.morphline.avro.ArrayInUnionTestRecord.Builder(other);
    }

    /**
     * RecordBuilder for ArrayInUnionTestRecord instances.
     */
    public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<ArrayInUnionTestRecord>
            implements org.apache.avro.data.RecordBuilder<ArrayInUnionTestRecord> {

        private java.util.List<java.lang.String> items;
        private java.util.List<java.lang.String> itemsInUnion;

        /** Creates a new Builder */
        private Builder() {
            super(org.kitesdk.morphline.avro.ArrayInUnionTestRecord.SCHEMA$);
        }

        /**
         * Creates a Builder by copying an existing Builder.
         * 
         * @param other
         *            The existing Builder to copy.
         */
        private Builder(final org.kitesdk.morphline.avro.ArrayInUnionTestRecord.Builder other) {
            super(other);
            if (isValidValue(fields()[0], other.items)) {
                this.items = data().deepCopy(fields()[0].schema(), other.items);
                fieldSetFlags()[0] = true;
            }
            if (isValidValue(fields()[1], other.itemsInUnion)) {
                this.itemsInUnion = data().deepCopy(fields()[1].schema(), other.itemsInUnion);
                fieldSetFlags()[1] = true;
            }
        }

        /**
         * Creates a Builder by copying an existing ArrayInUnionTestRecord instance
         * 
         * @param other
         *            The existing instance to copy.
         */
        private Builder(final org.kitesdk.morphline.avro.ArrayInUnionTestRecord other) {
            super(org.kitesdk.morphline.avro.ArrayInUnionTestRecord.SCHEMA$);
            if (isValidValue(fields()[0], other.items)) {
                this.items = data().deepCopy(fields()[0].schema(), other.items);
                fieldSetFlags()[0] = true;
            }
            if (isValidValue(fields()[1], other.itemsInUnion)) {
                this.itemsInUnion = data().deepCopy(fields()[1].schema(), other.itemsInUnion);
                fieldSetFlags()[1] = true;
            }
        }

        /**
         * Gets the value of the 'items' field.
         * 
         * @return The value.
         */
        public java.util.List<java.lang.String> getItems() {
            return items;
        }

        /**
         * Sets the value of the 'items' field.
         * 
         * @param value
         *            The value of 'items'.
         * @return This builder.
         */
        public org.kitesdk.morphline.avro.ArrayInUnionTestRecord.Builder setItems(
                final java.util.List<java.lang.String> value) {
            validate(fields()[0], value);
            this.items = value;
            fieldSetFlags()[0] = true;
            return this;
        }

        /**
         * Checks whether the 'items' field has been set.
         * 
         * @return True if the 'items' field has been set, false otherwise.
         */
        public boolean hasItems() {
            return fieldSetFlags()[0];
        }

        /**
         * Clears the value of the 'items' field.
         * 
         * @return This builder.
         */
        public org.kitesdk.morphline.avro.ArrayInUnionTestRecord.Builder clearItems() {
            items = null;
            fieldSetFlags()[0] = false;
            return this;
        }

        /**
         * Gets the value of the 'itemsInUnion' field.
         * 
         * @return The value.
         */
        public java.util.List<java.lang.String> getItemsInUnion() {
            return itemsInUnion;
        }

        /**
         * Sets the value of the 'itemsInUnion' field.
         * 
         * @param value
         *            The value of 'itemsInUnion'.
         * @return This builder.
         */
        public org.kitesdk.morphline.avro.ArrayInUnionTestRecord.Builder setItemsInUnion(
                final java.util.List<java.lang.String> value) {
            validate(fields()[1], value);
            this.itemsInUnion = value;
            fieldSetFlags()[1] = true;
            return this;
        }

        /**
         * Checks whether the 'itemsInUnion' field has been set.
         * 
         * @return True if the 'itemsInUnion' field has been set, false otherwise.
         */
        public boolean hasItemsInUnion() {
            return fieldSetFlags()[1];
        }

        /**
         * Clears the value of the 'itemsInUnion' field.
         * 
         * @return This builder.
         */
        public org.kitesdk.morphline.avro.ArrayInUnionTestRecord.Builder clearItemsInUnion() {
            itemsInUnion = null;
            fieldSetFlags()[1] = false;
            return this;
        }

        @Override
        public ArrayInUnionTestRecord build() {
            try {
                final ArrayInUnionTestRecord record = new ArrayInUnionTestRecord();
                record.items = fieldSetFlags()[0] ? this.items
                        : (java.util.List<java.lang.String>) defaultValue(fields()[0]);
                record.itemsInUnion = fieldSetFlags()[1] ? this.itemsInUnion
                        : (java.util.List<java.lang.String>) defaultValue(fields()[1]);
                return record;
            } catch (final Exception e) {
                throw new org.apache.avro.AvroRuntimeException(e);
            }
        }
    }
}
