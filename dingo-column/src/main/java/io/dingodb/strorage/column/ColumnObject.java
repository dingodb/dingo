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
 * ColumnObject is an implementation of {@link AbstractNativeReference} which
 * has an immutable and therefore thread-safe reference to the underlying
 * native C++ Column object.
 * <p>
 * ColumnObject is the base-class of almost all Column classes that have a
 * pointer to some underlying native C++ {@code rocksdb} object.</p>
 * <p>
 * The use of {@code ColumnObject} should always be preferred over
 */
public abstract class ColumnObject extends AbstractImmutableNativeReference {

  /**
   * An immutable reference to the value of the C++ pointer pointing to some
   * underlying native Column C++ object.
   */
  protected final long nativeHandle_;

  protected ColumnObject(final long nativeHandle) {
    super(true);
    this.nativeHandle_ = nativeHandle;
  }

  /**
   * Deletes underlying C++ object pointer.
   */
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  protected abstract void disposeInternal(final long handle);

  public long getNativeHandle() {
    return nativeHandle_;
  }
}
