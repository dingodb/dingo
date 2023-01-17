/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.strorage.column;

/**
 * AbstractNativeReference is the base-class of all RocksDB classes that have
 * a pointer to a native C++ {@code rocksdb} object.
 * <p>
 * AbstractNativeReference has the {@link AbstractNativeReference#close()}
 * method, which frees its associated C++ object.</p>
 * <p>
 * This function should be called manually, or even better, called implicitly using a
 * <a
 * href="https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html">try-with-resources</a>
 * statement, when you are finished with the object. It is no longer
 * called automatically during the regular Java GC process via
 * {@link AbstractNativeReference#finalize()}.</p>
 * <p>
 * Explanatory note - When or if the Garbage Collector calls {@link Object#finalize()}
 * depends on the JVM implementation and system conditions, which the programmer
 * cannot control. In addition, the GC cannot see through the native reference
 * long member variable (which is the C++ pointer value to the native object),
 * and cannot know what other resources depend on it.
 * </p>
 */
public abstract class AbstractNativeReference implements AutoCloseable {
  /**
   * Returns true if we are responsible for freeing the underlying C++ object
   *
   * @return true if we are responsible to free the C++ object
   */
  protected abstract boolean isOwningHandle();

  /**
   * Frees the underlying C++ object
   * <p>
   * It is strong recommended that the developer calls this after they
   * have finished using the object.</p>
   * <p>
   * Note, that once an instance of {@link AbstractNativeReference} has been
   * closed, calling any of its functions will lead to undefined
   * behavior.</p>
   */
  @Override public abstract void close();
}
