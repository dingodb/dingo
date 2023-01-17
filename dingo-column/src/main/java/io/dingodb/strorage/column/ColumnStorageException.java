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
 * A ColumnStorageException encapsulates the error of an operation.  This exception
 * type is used to describe an internal error from the c++ rocksdb library.
 */
public class ColumnStorageException extends Exception {

  /* @Nullable */ private final Status status;

  /**
   * The private construct used by a set of public static factory method.
   *
   * @param msg the specified error message.
   */
  public ColumnStorageException(final String msg) {
    this(msg, null);
  }

  public ColumnStorageException(final String msg, final Status status) {
    super(msg);
    this.status = status;
  }

  public ColumnStorageException(final Status status) {
    super(status.getState() != null ? status.getState()
        : status.getCodeString());
    this.status = status;
  }

  /**
   * Get the status returned from RocksDB
   *
   * @return The status reported by RocksDB, or null if no status is available
   */
  public Status getStatus() {
    return status;
  }
}
