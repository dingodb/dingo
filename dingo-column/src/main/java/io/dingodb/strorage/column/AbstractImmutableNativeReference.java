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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Offers functionality for implementations of
 * {@link AbstractNativeReference} which have an immutable reference to the
 * underlying native C++ object
 */
//@ThreadSafe
public abstract class AbstractImmutableNativeReference
    extends AbstractNativeReference {

  /**
   * A flag indicating whether the current {@code AbstractNativeReference} is
   * responsible to free the underlying C++ object
   */
  protected final AtomicBoolean owningHandle_;

  protected AbstractImmutableNativeReference(final boolean owningHandle) {
    this.owningHandle_ = new AtomicBoolean(owningHandle);
  }

  @Override
  public boolean isOwningHandle() {
    return owningHandle_.get();
  }

  /**
   * Releases this {@code AbstractNativeReference} from  the responsibility of
   * freeing the underlying native C++ object
   * <p>
   * This will prevent the object from attempting to delete the underlying
   * native object in {@code close()}. This must be used when another object
   * takes over ownership of the native object or both will attempt to delete
   * the underlying object when closed.
   * <p>
   * When {@code disOwnNativeHandle()} is called, {@code close()} will
   * subsequently take no action. As a result, incorrect use of this function
   * may cause a memory leak.
   * </p>
   */
  protected final void disOwnNativeHandle() {
    owningHandle_.set(false);
  }

  @Override
  public void close() {
    if (owningHandle_.compareAndSet(true, false)) {
      disposeInternal();
    }
  }

  /**
   * The helper function of {@link AbstractImmutableNativeReference#close()}
   * which all subclasses of {@code AbstractImmutableNativeReference} must
   * implement to release their underlying native C++ objects.
   */
  protected abstract void disposeInternal();
}
