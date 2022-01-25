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

package io.dingodb.raft;

import com.codahale.metrics.MetricRegistry;
import io.dingodb.raft.core.NodeMetrics;
import io.dingodb.raft.util.FileOutputSignalHandler;
import io.dingodb.raft.util.MetricReporter;
import io.dingodb.raft.util.SystemPropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class NodeMetricsSignalHandler extends FileOutputSignalHandler {
    private static Logger LOG = LoggerFactory.getLogger(NodeMetricsSignalHandler.class);

    private static final String DIR = SystemPropertyUtil.get("jraft.signal.node.metrics.dir", "");
    private static final String BASE_NAME = "node_metrics.log";

    @Override
    public void handle(final String signalName) {
        final List<Node> nodes = NodeManager.getInstance().getAllNodes();
        if (nodes.isEmpty()) {
            return;
        }

        try {
            final File file = getOutputFile(DIR, BASE_NAME);

            LOG.info("Printing raft nodes metrics with signal: {} to file: {}.", signalName, file);

            try (final PrintStream out = new PrintStream(new FileOutputStream(file, true))) {
                for (final Node node : nodes) {
                    final NodeMetrics nodeMetrics = node.getNodeMetrics();
                    final MetricRegistry registry = nodeMetrics.getMetricRegistry();
                    if (registry == null) {
                        LOG.warn("Node: {} received a signal to print metric, but it does not have metric enabled.",
                            node);
                        continue;
                    }
                    final MetricReporter reporter = MetricReporter.forRegistry(registry) //
                        .outputTo(out) //
                        .prefixedWith("-- " + node.getNodeId()) //
                        .build();
                    reporter.report();
                }
            }
        } catch (final IOException e) {
            LOG.error("Fail to print nodes metrics: {}.", nodes, e);
        }
    }
}
