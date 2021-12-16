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

package io.dingodb.dingokv;

import com.alipay.sofa.jraft.util.Describer;
import com.alipay.sofa.jraft.util.FileOutputSignalHandler;
import com.alipay.sofa.jraft.util.SystemPropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class DingoKVDescribeSignalHandler extends FileOutputSignalHandler {
    private static Logger LOG = LoggerFactory.getLogger(DingoKVDescribeSignalHandler.class);

    private static final String DIR = SystemPropertyUtil.get("dingokv.signal.describe.dir", "");
    private static final String BASE_NAME = "dingokv_describe.log";

    @Override
    public void handle(final String signalName) {
        final List<Describer> describers = DescriberManager.getInstance().getAllDescribers();
        if (describers.isEmpty()) {
            return;
        }

        try {
            final File file = getOutputFile(DIR, BASE_NAME);

            LOG.info("Describing dingokv with signal: {} to file: {}.", signalName, file);

            try (final PrintWriter out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file, true),
                StandardCharsets.UTF_8))) {
                final Describer.Printer printer = new Describer.DefaultPrinter(out);
                for (final Describer describer : describers) {
                    describer.describe(printer);
                }
            }
        } catch (final IOException e) {
            LOG.error("Fail to describe dingokv: {}.", describers, e);
        }
    }
}
