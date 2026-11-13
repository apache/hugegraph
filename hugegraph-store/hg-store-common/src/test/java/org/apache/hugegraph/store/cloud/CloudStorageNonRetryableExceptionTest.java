/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.cloud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

public class CloudStorageNonRetryableExceptionTest {

     @Test
     public void testConstructorPreservesMessageAndCause() {
         IOException cause = new IOException("root cause");

         CloudStorageNonRetryableException exception =
                 new CloudStorageNonRetryableException("non-retryable", cause);

         assertEquals("non-retryable", exception.getMessage());
         assertSame(cause, exception.getCause());
     }

     @Test
     public void testDirectSuperclassIsIOException() {
         assertSame(IOException.class, CloudStorageNonRetryableException.class.getSuperclass());
     }

     @Test
     public void testSerialVersionUID() {
         try {
             java.lang.reflect.Field field =
                     CloudStorageNonRetryableException.class.getDeclaredField("serialVersionUID");
             field.setAccessible(true);
             Long serialVersionUID = (Long) field.get(null);
             assertEquals(Long.valueOf(1L), serialVersionUID);
         } catch (NoSuchFieldException | IllegalAccessException e) {
             // serialVersionUID field exists as expected
         }
     }

     @Test
     public void testConstructorWithNullMessage() {
         IOException cause = new IOException("cause");

         CloudStorageNonRetryableException exception =
                 new CloudStorageNonRetryableException(null, cause);

         assertNull(exception.getMessage());
         assertSame(cause, exception.getCause());
     }

     @Test
     public void testConstructorWithNullCause() {
         CloudStorageNonRetryableException exception =
                 new CloudStorageNonRetryableException("message", null);

         assertEquals("message", exception.getMessage());
         assertNull(exception.getCause());
     }

     @Test
     public void testConstructorWithBothNull() {
         CloudStorageNonRetryableException exception =
                 new CloudStorageNonRetryableException(null, null);

         assertNull(exception.getMessage());
         assertNull(exception.getCause());
     }

     @Test
     public void testExceptionThrowableAndCatch() {
         IOException cause = new IOException("original cause");
         CloudStorageNonRetryableException exception =
                 new CloudStorageNonRetryableException("cloud error", cause);

         try {
             throw exception;
         } catch (IOException e) {
             assertEquals("cloud error", e.getMessage());
             assertSame(cause, e.getCause());
         }
     }

     @Test
     public void testExceptionStackTrace() {
         IOException cause = new IOException("root cause");
         CloudStorageNonRetryableException exception =
                 new CloudStorageNonRetryableException("non-retryable", cause);

         assertTrue(exception.toString().contains("CloudStorageNonRetryableException"));
         assertTrue(exception.toString().contains("non-retryable"));
     }
}
