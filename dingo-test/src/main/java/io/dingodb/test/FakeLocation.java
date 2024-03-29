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

package io.dingodb.test;

import io.dingodb.common.Location;

public class FakeLocation extends Location {
    private static final long serialVersionUID = 3123677892588410633L;

    private final int id;

    public FakeLocation(int id, int port) {
        super("127.0.0.1", port);
        this.id = id;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof FakeLocation) {
            return this.id == ((FakeLocation) other).id;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
