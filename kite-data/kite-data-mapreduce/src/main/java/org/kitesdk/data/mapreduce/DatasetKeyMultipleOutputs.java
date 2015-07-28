/**
 * Copyright 2014 Cloudera Inc.
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
package org.kitesdk.data.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.kitesdk.data.*;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;

/**
 * The DatasetKeyMultipleOutputs class simplifies writing output data
 * to multiple outputs. The class allows use of various file formats such as Parquet, Avro, CSV etc. using Kite APIs
 *
 * <p>
 * Case one: writing to additional outputs other than the job default output.
 *
 * Each additional output, or named output, may be configured with its own
 * <code>Schema</code>, <code>Formats</code>, <code>CompressionType</code>.
 * </p>
 * <p>
 * Case two: to write data to different files provided by user
 * </p>
 **
 * Usage pattern for job submission:
 * <pre>
 *
 *  Job job = Job.getInstance(getConf());
 *  job.setJarByClass(getClass());
 *
 * LazyOutputFormat.setOutputFormatClass(job,TextOutputFormat.class);
 * FileInputFormat.addInputPath(job, new Path(args[1]));
 * FileOutputFormat.setOutputPath(job,new Path(args[2]));
 *
 * DatasetKeyMultipleOutputs.addNamedOutput(job,"WordCount1",
 *                              Words.getClassSchema(),Words.class,
 *                              HDFS_URI_PREFIX + args[2].toString() + "/WordCount1",
 *                              CompressionType.Snappy);
 * DatasetKeyMultipleOutputs.addNamedOutput(job,"WordCount2",
 *                              Words.getClassSchema(),Words.class,
 *                              HDFS_URI_PREFIX + args[2].toString() + "/WordCount2",
 *                              CompressionType.Bzip2,
 *                              Formats.AVRO);
 * ...
 *
 * ...
 *
 * job.waitForCompletion(true);
 * ...
 * </pre>
 * <p>
 * Usage in Reducer:
 * <pre>
 *
 * public class MRWordCountReducer extends Reducer<Text,IntWritable,Void,Void> {
 *
 * private DatasetKeyMultipleOutputs mos;
 *
 *
 * @Override
 * public void setup(Context context) {
 * mos = new DatasetKeyMultipleOutputs(context);
 * }
 * ...
 *
 * @Override
 * public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
 *
 * int count = 0;
 * for (IntWritable v: values)
 * count++;
 *
 * mos.write("WordCount1",new Words(key.toString(),count));
 * mos.write("WordCount2",new Words(key.toString(),count));
 *
 * }
 * ...
 *
 * @Override
 * public void cleanup(Context context) throws IOException,InterruptedException {
 * mos.close();
 * }
 * }
 * </pre>
 */
public class DatasetKeyMultipleOutputs {
    private static final String MULTIPLE_OUTPUTS =
            "kite.mapreduce.multipleoutputs";
    private static final String MO_PREFIX =
            "kite.mapreduce.multipleoutputs.namedOutput.";
    private static final String COMPRESSION =
            ".compression";
    private static final String FORMAT =
            ".format";
    private static final String URIS =
            ".uri";
    private static final String MO_TYPE =
            ".type";

    // Map of task contexts,DatasetDescriptors & Datasets
    private Map<String, TaskAttemptContext> taskContexts = new HashMap<String, TaskAttemptContext>();
    private Map<String, DatasetDescriptor> datasetDescriptors = new HashMap<String, DatasetDescriptor>();
    private Map<String, Dataset<?>> dataSets = new HashMap<String, Dataset<?>>();

    /**
     * Checks if a named output name is valid token.
     *
     * @param namedOutput named output Name
     * @throws IllegalArgumentException if the output name is not valid.
     */
    private static void checkTokenName(String namedOutput) {
        if (namedOutput == null || namedOutput.length() == 0) {
            throw new IllegalArgumentException(
                    "Name cannot be NULL or empty");
        }
        for (char ch : namedOutput.toCharArray()) {
            if ((ch >= 'A') && (ch <= 'Z')) {
                continue;
            }
            if ((ch >= 'a') && (ch <= 'z')) {
                continue;
            }
            if ((ch >= '0') && (ch <= '9')) {
                continue;
            }
            throw new IllegalArgumentException(
                    "Name cannot have a '" + ch + "' char");
        }
    }

    /**
     * Checks if output name is valid.
     *
     * name cannot be the name used for the default output
     * @param outputPath base output Name
     * @throws IllegalArgumentException if the output name is not valid.
     */
    private static void checkBaseOutputPath(String outputPath) {
        if (outputPath.equals("part")) {
            throw new IllegalArgumentException("output name cannot be 'part'");
        }
    }

    /**
     * Checks if a named output name is valid.
     *
     * @param namedOutput named output Name
     * @throws IllegalArgumentException if the output name is not valid.
     */
    private static void checkNamedOutputName(JobContext job,
                                             String namedOutput, boolean alreadyDefined) {
        checkTokenName(namedOutput);
        checkBaseOutputPath(namedOutput);
        List<String> definedChannels = getNamedOutputsList(job);
        if (alreadyDefined && definedChannels.contains(namedOutput)) {
            throw new IllegalArgumentException("Named output '" + namedOutput +
                    "' already alreadyDefined");
        } else if (!alreadyDefined && !definedChannels.contains(namedOutput)) {
            throw new IllegalArgumentException("Named output '" + namedOutput +
                    "' not defined");
        }
    }

    // Returns list of channel names.
    private static List<String> getNamedOutputsList(JobContext job) {
        List<String> names = new ArrayList<String>();
        StringTokenizer st = new StringTokenizer(
                job.getConfiguration().get(MULTIPLE_OUTPUTS, ""), " ");
        while (st.hasMoreTokens()) {
            names.add(st.nextToken());
        }
        return names;
    }


    /**
     * Adds a named output for the job.
     * <p/>
     *
     * @param job               job to add the named output
     * @param namedOutput       named output name, it has to be a word, letters
     *                          and numbers only, cannot be the word 'part' as
     *                          that is reserved for the default output.
     * @param keySchema          Schema for the Key
     * @param tClass            Key object class
     * @param uriPath           URI location for the named output
     */
    @SuppressWarnings("unchecked")
    public static void addNamedOutput(Job job,
                                      String namedOutput,
                                      Schema keySchema,
                                      Class<? extends IndexedRecord> tClass,
                                      String uriPath
    ) {
        addNamedOutput(job,namedOutput,keySchema,tClass,uriPath,CompressionType.Snappy,Formats.PARQUET);
    }

    /**
     * Adds a named output for the job.
     * <p/>
     *
     * @param job               job to add the named output
     * @param namedOutput       named output name, it has to be a word, letters
     *                          and numbers only, cannot be the word 'part' as
     *                          that is reserved for the default output.
     * @param keySchema          Schema for the Key
     * @param tClass            Key object class
     * @param uriPath           URI location for the named output
     * @param compressionType   Compression Type
     */
    @SuppressWarnings("unchecked")
    public static void addNamedOutput(Job job,
                                      String namedOutput,
                                      Schema keySchema,
                                      Class<? extends IndexedRecord> tClass,
                                      String uriPath,
                                      CompressionType compressionType
    ) {
        addNamedOutput(job,namedOutput,keySchema,tClass,uriPath,compressionType,Formats.PARQUET);
    }

    /**
     * Adds a named output for the job.
     * <p/>
     *
     * @param job               job to add the named output
     * @param namedOutput       named output name, it has to be a word, letters
     *                          and numbers only, cannot be the word 'part' as
     *                          that is reserved for the default output.
     * @param keySchema          Schema for the Key
     * @param tClass            Key object class
     * @param uriPath           URI location for the named output
     * @param compressionType   compressionType for storage
     * @param format            Storage format
     */
    @SuppressWarnings("unchecked")
    public static void addNamedOutput(Job job,
                                      String namedOutput,
                                      Schema keySchema,
                                      Class<? extends IndexedRecord> tClass,
                                      String uriPath,
                                      CompressionType compressionType,
                                      Format format) {
        checkNamedOutputName(job, namedOutput, true);
        Configuration conf = job.getConfiguration();

        conf.set(MULTIPLE_OUTPUTS,
                conf.get(MULTIPLE_OUTPUTS, "") + " " + namedOutput);

        conf.set(MO_PREFIX + namedOutput + URIS, uriPath);

        conf.set(MO_PREFIX + namedOutput + ".keyschema", keySchema.toString());

        conf.setClass(MO_PREFIX + namedOutput + MO_TYPE,tClass,tClass);

        conf.set(MO_PREFIX + namedOutput + COMPRESSION,compressionType.getName());

        conf.set(MO_PREFIX + namedOutput + FORMAT, format.getName());

    }

    private TaskInputOutputContext<?, ?, ?, ?> context;
    private Set<String> namedOutputs;
    private Map<String, DatasetWriter<?>> dataSetWriters;


    /**
     * Creates and initializes multiple named outputs support, it should be
     * instantiated in the Mapper/Reducer configure method.
     *
     * @param context the mapper/reducer context object
     */
    public DatasetKeyMultipleOutputs(
            TaskInputOutputContext<?, ?, ?, ?> context) {
        this.context = context;
        namedOutputs = Collections.unmodifiableSet(
                new HashSet<String>(DatasetKeyMultipleOutputs.getNamedOutputsList(context)));
        dataSetWriters = new HashMap<String, DatasetWriter<?>>();

    }

    /**
     * Write key and value to the namedOutput.
     *
     * Output path is a unique file generated for the namedOutput.
     * For example, {namedOutput}-(m|r)-{part-number}
     *
     * @param namedOutput the named output name
     * @param key         the key , value is NullWritable
     */
    @SuppressWarnings("unchecked")
    public void write(String namedOutput, Object key)
            throws IOException, InterruptedException {
        write(namedOutput, key, NullWritable.get(), namedOutput);
    }

    /**
     * Write key and value to baseOutputPath using the namedOutput.
     *
     * @param namedOutput    the named output name
     * @param key            the key
     * @param value          the value
     * @param baseOutputPath base-output path to write the record to.
     * Note: Framework will generate unique filename for the baseOutputPath
     */
    @SuppressWarnings("unchecked")
    public void write(String namedOutput, Object key, Object value,
                      String baseOutputPath) throws IOException, InterruptedException {
        checkNamedOutputName(context, namedOutput, false);
        checkBaseOutputPath(baseOutputPath);
        if (!namedOutputs.contains(namedOutput)) {
            throw new IllegalArgumentException("Undefined named output '" +
                    namedOutput + "'");
        }
        TaskAttemptContext taskContext = getContext(namedOutput);


        getRecordWriter(taskContext, namedOutput).write(key);
    }



    @SuppressWarnings("unchecked")
    private synchronized <E> DatasetWriter<E> getRecordWriter(
            TaskAttemptContext taskContext, String namedOutput)
            throws IOException, InterruptedException {

        // look for record-writer in the cache
        DatasetWriter writer = dataSetWriters.get(namedOutput);

        // If not in cache, create a new one
        if (writer == null) {
            // get the record writer from context output format
            Configuration conf = taskContext.getConfiguration();
            Dataset ds = getDataset(namedOutput);
            DatasetKeyOutputFormat.ConfigBuilder builder = DatasetKeyOutputFormat.configure(conf);
            builder.appendTo(ds.getUri());
            builder.withType(ds.getSchema().getClass());

            writer = ds.newWriter();


            // add the record-writer to the cache
            dataSetWriters.put(namedOutput, writer);
        }
        return writer;
    }



    @SuppressWarnings("deprecation")
    private TaskAttemptContext getContext(String nameOutput) throws IOException {

        TaskAttemptContext taskContext = taskContexts.get(nameOutput);

        if (taskContext != null) {
            return taskContext;
        }

        // The following trick leverages the instantiation of a record writer via
        // the job thus supporting arbitrary output formats.

        Job job = new Job(context.getConfiguration());
        job.setOutputFormatClass(DatasetKeyOutputFormat.class);

        taskContext = createTaskAttemptContext(
                job.getConfiguration(), context.getTaskAttemptID());

        taskContexts.put(nameOutput, taskContext);

        return taskContext;
    }

    private TaskAttemptContext createTaskAttemptContext(Configuration conf,
                                                        TaskAttemptID taskId) {
        // Use reflection since the context types changed incompatibly between 1.0
        // and 2.0.
        try {
            Class<?> c = getTaskAttemptContextClass();
            Constructor<?> cons = c.getConstructor(Configuration.class,
                    TaskAttemptID.class);
            return (TaskAttemptContext) cons.newInstance(conf, taskId);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }



    private Class<?> getTaskAttemptContextClass() {
        try {
            return Class.forName(
                    "org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
        } catch (Exception e) {
            try {
                return Class.forName(
                        "org.apache.hadoop.mapreduce.TaskAttemptContext");
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        }

    }


    private <E extends IndexedRecord> Dataset getDataset(String namedOutput) {
        Dataset dataset = dataSets.get(namedOutput);

        if ( dataset != null ) {
            return dataset;
        }

        DatasetDescriptor datasetDescriptor = getDatasetDescriptor(namedOutput);
        String uri = null;
        if (context.getConfiguration().get(MO_PREFIX + namedOutput + URIS) != null)
            uri = context.getConfiguration().get(MO_PREFIX + namedOutput + URIS);
        else {
            throw new IllegalArgumentException("URI for namedoutput " + namedOutput + " not defined!");
        }

        Class<? extends E> tClass;
        try {
            tClass = (Class<E>)context.getConfiguration().getClass(MO_PREFIX + namedOutput + MO_TYPE, GenericData.Record.class);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof ClassNotFoundException) {
                throw new TypeNotFoundException(String.format(
                        "The Java class %s for the entity type could not be found",
                        context.getConfiguration().get(MO_PREFIX + namedOutput + MO_TYPE)),
                        e.getCause());
            } else {
                throw e;
            }
        }


        dataset = createOrLoadDataset(uri, datasetDescriptor, tClass);

        dataSets.put(namedOutput,dataset);

        return dataset;
    }

    private DatasetDescriptor getDatasetDescriptor(String namedOutput) {
        DatasetDescriptor datasetDescriptor = datasetDescriptors.get(namedOutput);

        if (datasetDescriptor != null) {
            return datasetDescriptor;
        }

        // Create a new descriptor if it doesnt exist
        Schema schema = null;
        CompressionType compressionType = null;
        Format format = null;

        if (context.getConfiguration().get(MO_PREFIX + namedOutput + ".keyschema") != null)
            schema = Schema.parse(context.getConfiguration().get(
                    MO_PREFIX + namedOutput + ".keyschema"));
        else {
            throw new IllegalArgumentException("Keyschema for namedoutput " + namedOutput + " not defined!");
        }

        if (context.getConfiguration().get(MO_PREFIX + namedOutput + COMPRESSION) != null)
            compressionType = CompressionType.forName(context.getConfiguration().get(MO_PREFIX + namedOutput + COMPRESSION));
        else {
            compressionType = CompressionType.Snappy;
        }

        if (context.getConfiguration().get(MO_PREFIX + namedOutput + FORMAT) != null)
            format = Formats.fromString(context.getConfiguration().get(MO_PREFIX + namedOutput + FORMAT));

        else {
            format = Formats.PARQUET;
        }

        datasetDescriptor = createDatasetDescriptor(schema,compressionType,format);

        datasetDescriptors.put(namedOutput,datasetDescriptor);

        return datasetDescriptor;
    }

    private DatasetDescriptor createDatasetDescriptor(Schema schema, CompressionType compressionType, Format format) {
        DatasetDescriptor datasetDescriptor = new DatasetDescriptor.Builder()
                .schema(schema)
                .format(format)
                .compressionType(compressionType)
                .build();
        return datasetDescriptor;
    }

    private synchronized Dataset createOrLoadDataset(String uri,DatasetDescriptor datasetDescriptor,Class<? extends IndexedRecord> tClass){
        if (Datasets.exists(uri))
            return Datasets.load(uri,tClass);
        else
            return Datasets.create(uri,datasetDescriptor,tClass);
    }

    @SuppressWarnings("unchecked")
    public void close() throws IOException, InterruptedException {
        for (DatasetWriter writer : dataSetWriters.values()) {
            writer.close();
        }
    }


}