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

package io.dingodb.raft.error;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class LogIndexOutOfBoundsException extends IndexOutOfBoundsException {
    private static final long serialVersionUID = -1096992049027390572L;

    /**
     * Constructs an <code>LogIndexOutOfBoundsException</code> with no
     * detail message.
     */
    public LogIndexOutOfBoundsException() {
        super();
    }

    /**
     * Constructs a new <code>LogIndexOutOfBoundsException</code>
     * class with an argument indicating the illegal index.
     *
     * @param   index   the illegal index.
     */
    public LogIndexOutOfBoundsException(int index) {
        super("Array index out of range: " + index);
    }

    /**
     * Constructs an <code>LogIndexOutOfBoundsException</code> class
     * with the specified detail message.
     *
     * @param   s   the detail message.
     */
    public LogIndexOutOfBoundsException(String s) {
        super(s);
    }
}
