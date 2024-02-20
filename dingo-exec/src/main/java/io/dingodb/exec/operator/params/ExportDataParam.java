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

package io.dingodb.exec.operator.params;

import lombok.Getter;

import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

@Getter
public class ExportDataParam extends AbstractParams {

    private final static byte[] EMPTY_SPACE = new byte[] {0x09};
    private final String outfile;
    private final byte[] terminated;
    private final String id;

    private byte[] enclosed;

    private final byte[] lineTerminated;

    private final byte[] lineStarting;

    private final byte[] escaped;

    private final String charset;

    private final Calendar localCalendar;

    public ExportDataParam(String outfile,
                           byte[] terminated,
                           String id,
                           String enclosed,
                           byte[] lineTerminated,
                           byte[] escaped,
                           String charset,
                           byte[] lineStarting,
                           TimeZone timeZone) {
        this.outfile = outfile;
        this.terminated = terminated;
        this.id = id;
        if (enclosed != null) {
            this.enclosed = enclosed.getBytes();
        }
        this.lineTerminated = lineTerminated;
        this.escaped = escaped;
        this.charset = charset;
        this.lineStarting = lineStarting;
        this.localCalendar = Calendar.getInstance(timeZone, Locale.ROOT);
    }
}
