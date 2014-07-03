/*
 * Copyright 2013 Cloudera.
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
package org.kitesdk.data;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import org.kitesdk.data.spi.InitializeAccessor;

import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.RecordValidator;
import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.checkReaderBehavior;
import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.checkReaderIteration;

/**
 * Tests for all DatasetReader implementations.
 *
 * This is not a @Parameterized test so that other implementations can also
 * use these tests. To apply these tests to a new DatasetReader implementation,
 * create a new test class that inherits from this one and implements the
 * abstract methods.
 *
 * @param <R> The type of entities returned by the reader.
 */
public abstract class TestDatasetReaders<R> {
  abstract public DatasetReader<R> newReader() throws IOException ;
  abstract public int getTotalRecords();
  abstract public RecordValidator<R> getValidator();

  private DatasetReader<R> reader = null;
  private int totalRecords = 0;
  private RecordValidator<R> validator = null;

  @Before
  public void setupReader() throws IOException {
    this.reader = newReader();
    this.totalRecords = getTotalRecords();
    this.validator = getValidator();
  }

  @Test
  public void testBasicBehavior() throws IOException {
    checkReaderBehavior(reader, totalRecords, validator);
  }

  @Test
  public void testDoubleCloseIgnored() throws IOException {
    checkReaderBehavior(reader, totalRecords, validator);
    reader.close();
  }

  @Test(expected = IllegalStateException.class)
  public void testHasNextOnClosedWriterFails() throws IOException {
    checkReaderBehavior(reader, totalRecords, validator);

    try {
      reader.hasNext();
    } finally {
      reader.close();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testNextOnClosedWriterFails() throws IOException {
    checkReaderBehavior(reader, totalRecords, validator);

    try {
      reader.next();
    } finally {
      reader.close();
    }
  }

  @Test
  public void testRemove() throws IOException {
    try {
      try {
        if (!reader.isOpen() && reader instanceof InitializeAccessor) {
          ((InitializeAccessor) reader).initialize();
        }
      } catch (Throwable t) {
        Assert.fail("Reader failed in open: " + t.getClass().getName());
      }

      Assert.assertTrue("Reader is not open after open()", reader.isOpen());

      try {
        reader.remove();
        Assert.fail("Remove before iteration succeeded");
      } catch (IllegalStateException ex) {
        // this is the expected behavior
      } catch (UnsupportedOperationException ex) {
        // this is okay, too
      }

      checkReaderIteration(reader, totalRecords, validator);

      try {
        // this could be a successful case, but we choose not to implement it;
        // see the implementation note for details.
        reader.remove();
        Assert.fail("Remove after last hasNext() succeeded");
      } catch (IllegalStateException ex) {
        // this is the expected behavior
      } catch (UnsupportedOperationException ex) {
        // this is okay, too
      }

    } finally {
      reader.close();
    }
  }
}
