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

package io.dingodb.calcite.grammar.dql;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ExportOptions {
    private boolean export;
    private String outfile;
    private byte[] terminated;

    private String enclosed;
    private byte[] lineTerminated;
    private byte[] escaped;
    private byte[] lineStarting;
    private String charset;

    public ExportOptions(boolean export,
                         String outfile,
                         byte[] terminated,
                         String enclosed,
                         byte[] lineTerminated,
                         byte[] escaped,
                         byte[] lineStarting,
                         String charset) {
        this.export = export;
        this.outfile = outfile;
        this.terminated = terminated;
        this.enclosed = enclosed;
        this.lineTerminated = lineTerminated;
        this.escaped = escaped;
        this.lineStarting = lineStarting;
        this.charset = charset;
    }

}
