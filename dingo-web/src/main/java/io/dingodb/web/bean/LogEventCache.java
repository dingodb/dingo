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

package io.dingodb.web.bean;

import java.util.LinkedHashMap;
import java.util.Map;

public class LogEventCache extends LinkedHashMap<String, String> {
    private static final long serialVersionUID = -5481683702090768361L;
    private final int capacity;

    public LogEventCache(int capacity) {
        super(capacity, 0.75f, true);
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
        return size() > capacity;
    }

    public static LogEventCache logCache = new LogEventCache(10000);
    public static LogEventCache eventCache = new LogEventCache(10000);
}
