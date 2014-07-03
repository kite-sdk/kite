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
package org.kitesdk.morphline.protobuf;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Validator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.typesafe.config.Config;

/**
 * Command that uses zero or more protocol buffer path expressions to extract
 * values from a protocol buffer object.
 * 
 * The protocol buffer input object is expected to be contained in the
 * {@link Fields#ATTACHMENT_BODY}
 * 
 * Each expression consists of a record output field name (on the left side of
 * the colon ':') as well as zero or more path steps (on the right hand side),
 * each path step separated by a '/' slash. Protocol buffer arrays are traversed
 * with the '[]' notation.
 * 
 * The result of a path expression is a list of objects, each of which is added
 * to the given record output field.
 * 
 * ExtractMethod option says what will results path to object. <li>toByteArray -
 * pass protocol buffer bytes to next command <li>toString - pass toString value
 * to next command <li>none - pass protocol buffer object to next command
 */
public final class ExtractProtobufPathsBuilder implements CommandBuilder {

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ExtractProtobufPaths(this, config, parent, child, context);
  }

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("extractProtobufPaths");
  }

  // /////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  // /////////////////////////////////////////////////////////////////////////////
  private static final class ExtractProtobufPaths extends AbstractCommand {

    private static final String OBJECT_EXTRACT_METHOD = "objectExtractMethod";

    private static final String ENUM_EXTRACT_METHOD = "enumExtractMethod";

    private static final String LIST = "List";
    private static final String GET = "get";
    private static final String HAS = "has";

    private static final String ARRAY_TOKEN = "[]";
    private static final Set<Class<?>> WRAPPERS;
    static {
      Set<Class<?>> ret = new HashSet<Class<?>>();
      ret.add(Boolean.class);
      ret.add(Character.class);
      ret.add(Byte.class);
      ret.add(Short.class);
      ret.add(Integer.class);
      ret.add(Long.class);
      ret.add(Float.class);
      ret.add(Double.class);
      ret.add(String.class);
      ret.add(byte[].class);
      WRAPPERS = Collections.<Class<?>> unmodifiableSet(ret);
    }
    private final ObjectExtractMethods objectExtractMethod;
    private final EnumExtractMethods enumExtractMethod;

    private final Map<String, Collection<String>> stepMap;
    private final Map<Class<?>, Method> objectExtractMethods = new HashMap<Class<?>, Method>();
    private final Map<Class<?>, Method> enumExtractMethods = new HashMap<Class<?>, Method>();
    private final Map<Class<?>, Map<String, Method>> propertyGetters = new HashMap<Class<?>, Map<String, Method>>();
    private final Map<Class<?>, Map<String, Method>> propertyCheckers = new HashMap<Class<?>, Map<String, Method>>();

    public ExtractProtobufPaths(CommandBuilder builder, Config config, Command parent, Command child,
        MorphlineContext context) {

      super(builder, config, parent, child, context);
      ListMultimap<String, String> stepMultiMap = ArrayListMultimap.create();

      this.objectExtractMethod = new Validator<ObjectExtractMethods>().validateEnum(config,
          getConfigs().getString(config, OBJECT_EXTRACT_METHOD, ObjectExtractMethods.toByteArray.name()),
          ObjectExtractMethods.class);
      this.enumExtractMethod = new Validator<EnumExtractMethods>()
          .validateEnum(config, getConfigs().getString(config, ENUM_EXTRACT_METHOD, EnumExtractMethods.name.name()),
              EnumExtractMethods.class);

      Config paths = getConfigs().getConfig(config, "paths");
      for (Map.Entry<String, Object> entry : new Configs().getEntrySet(paths)) {
        String fieldName = entry.getKey();
        String path = entry.getValue().toString().trim();
        if (path.contains("//")) {
          throw new MorphlineCompilationException("No support for descendant axis available yet", config);
        }
        if (path.startsWith("/")) {
          path = path.substring(1);
        }
        if (path.endsWith("/")) {
          path = path.substring(0, path.length() - 1);
        }
        path = path.trim();
        for (String step : path.split("/")) {
          step = step.trim();
          if (step.length() > ARRAY_TOKEN.length() && step.endsWith(ARRAY_TOKEN)) {
            step = step.substring(0, step.length() - ARRAY_TOKEN.length());
            stepMultiMap.put(fieldName, normalize(step));
            stepMultiMap.put(fieldName, ARRAY_TOKEN);
          } else {
            stepMultiMap.put(fieldName, normalize(step));
          }
        }
      }
      this.stepMap = stepMultiMap.asMap();
      LOG.debug("stepMap: {}", stepMap);
      validateArguments();
    }

    @Override
    protected boolean doProcess(Record inputRecord) {
      Object datum = inputRecord.getFirstValue(Fields.ATTACHMENT_BODY);
      Preconditions.checkNotNull(datum);
      Record outputRecord = inputRecord.copy();

      for (Map.Entry<String, Collection<String>> entry : stepMap.entrySet()) {
        String fieldName = entry.getKey();
        List<String> steps = (List<String>) entry.getValue();
        try {
          extractPath(datum, fieldName, steps, outputRecord, 0);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
          return false;
        }
      }

      // pass record to next command in chain:
      return getChild().process(outputRecord);
    }

    private void extractPath(Object datum, String fieldName, List<String> steps, Record record, int level)
        throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
        InvocationTargetException {
      if (level >= steps.size()) {
        return;
      }
      boolean isLeaf = (level + 1 == steps.size());
      String step = steps.get(level);
      if (ARRAY_TOKEN == step) {
        if (!List.class.isAssignableFrom(datum.getClass())) {
          throw new MorphlineRuntimeException("Datum " + datum + " is not a list. Steps: " + steps + " Level: " + level);
        }
        if (isLeaf) {
          resolve(datum, record, fieldName);
        } else {
          Iterator<?> iter = ((List<?>) datum).iterator();
          while (iter.hasNext()) {
            extractPath(iter.next(), fieldName, steps, record, level + 1);
          }
        }
      } else if (hasProperty(datum, step)) {
        Object value = readProperty(datum, step);
        if (value != null) {
          if (isLeaf) {
            resolve(value, record, fieldName);
          } else {
            extractPath(value, fieldName, steps, record, level + 1);
          }
        }
      }
    }

    private Object extractValue(Object datum, Class<?> clazz) throws IllegalAccessException, IllegalArgumentException,
        InvocationTargetException, NoSuchMethodException, SecurityException {

      boolean isEnum = clazz.isEnum();

      if (isEnum && enumExtractMethod == EnumExtractMethods.none) {
        return datum;
      } else if (objectExtractMethod == ObjectExtractMethods.none) {
        return datum;
      }

      Map<Class<?>, Method> extractMethods;
      String methodName;
      if (isEnum) {
        extractMethods = enumExtractMethods;
        methodName = enumExtractMethod.name();
      } else {
        extractMethods = objectExtractMethods;
        methodName = objectExtractMethod.name();
      }

      Method extractMethod = extractMethods.get(clazz);
      if (extractMethod == null) {
        extractMethod = clazz.getMethod(methodName);
        extractMethods.put(clazz, extractMethod);
      }
      return extractMethod.invoke(datum);
    }

    private void findGetAndCheckMethods(String propertyName, Class<?> clazz) {
      Map<String, Method> getters = propertyGetters.get(clazz);
      Map<String, Method> checkers = propertyCheckers.get(clazz);
      if (getters == null) {
        getters = new HashMap<String, Method>();
        checkers = new HashMap<String, Method>();
        propertyGetters.put(clazz, getters);
        propertyCheckers.put(clazz, checkers);
      }
      if (!getters.containsKey(propertyName)) {
        String uppercaseProperty = new StringBuilder(propertyName.substring(0, 1).toUpperCase(Locale.ROOT)).append(
            propertyName.substring(1)).toString();

        String checkerName = new StringBuilder(HAS).append(uppercaseProperty).toString();
        try {
          checkers.put(propertyName, clazz.getMethod(checkerName));
        } catch (Exception e) {
          checkers.put(propertyName, null);
        }

        StringBuilder getterName = new StringBuilder(GET).append(uppercaseProperty);
        try {
          getters.put(propertyName, clazz.getMethod(getterName.toString()));
        } catch (NoSuchMethodException e) {
          getterName.append(LIST);
          try {
            getters.put(propertyName, clazz.getMethod(getterName.toString()));
          } catch (Exception e1) {
            throw new MorphlineRuntimeException("Property '" + propertyName + "' does not exist in class '"
                + clazz.getName() + "'.");
          }
        }
      }
    }

    private Method getCheckMethod(String propertyName, Class<?> clazz) {
      if (!propertyCheckers.containsKey(clazz) || !propertyCheckers.get(clazz).containsKey(propertyName)) {
        findGetAndCheckMethods(propertyName, clazz);
      }
      return propertyCheckers.get(clazz).get(propertyName);
    }

    private Method getReadMethod(String propertyName, Class<?> clazz) {
      if (!propertyGetters.containsKey(clazz) || !propertyGetters.get(clazz).containsKey(propertyName)) {
        findGetAndCheckMethods(propertyName, clazz);
      }
      return propertyGetters.get(clazz).get(propertyName);
    }

    private boolean hasProperty(Object datum, String propertyName) throws IllegalAccessException,
        IllegalArgumentException, InvocationTargetException {
      Method checker = getCheckMethod(propertyName, datum.getClass());
      return checker == null || Boolean.TRUE.equals(checker.invoke(datum));
    }

    private boolean isCommonType(Class<?> clazz) {
      return clazz.isPrimitive() || isWrapper(clazz);
    }

    private boolean isWrapper(Class<?> clazz) {
      return WRAPPERS.contains(clazz);
    }

    private String normalize(String step) { // for faster subsequent query
                                            // performance
      return ARRAY_TOKEN.equals(step) ? ARRAY_TOKEN : step;
    }

    private Object readProperty(Object datum, String propertyName) throws IllegalAccessException,
        IllegalArgumentException, InvocationTargetException {
      Method getter = getReadMethod(propertyName, datum.getClass());
      return getter.invoke(datum);
    }

    private void resolve(Object datum, Record record, String fieldName) throws NoSuchMethodException,
        SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
      if (datum == null) {
        return;
      }

      Class<?> clazz = datum.getClass();
      if (isCommonType(clazz)) {
        record.put(fieldName, datum);
      } else if (List.class.isAssignableFrom(clazz)) {
        for (Object o : (List<?>) datum) {
          resolve(o, record, fieldName);
        }
      } else {
        Object extracted = extractValue(datum, clazz);
        record.put(fieldName, extracted);
      }
    }

    private static enum EnumExtractMethods {
      name, toString, none
    }

    private static enum ObjectExtractMethods {
      toByteArray, toString, none
    }
  }

}
