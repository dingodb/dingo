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

import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.core.NodeImpl;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.option.BootstrapOptions;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.raft.util.JRaftServiceLoader;
import io.dingodb.raft.util.NamedThreadFactory;
import io.dingodb.raft.util.timer.RaftTimerFactory;
import org.apache.commons.lang.StringUtils;

import java.util.concurrent.ThreadFactory;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class JRaftUtils {
    private static final RaftTimerFactory TIMER_FACTORY = JRaftServiceLoader.load(RaftTimerFactory.class) //
                                                            .first();

    /**
     * Get raft timer factory.
     *
     * @return {@link RaftTimerFactory}
     */
    public static RaftTimerFactory raftTimerFactory() {
        return TIMER_FACTORY;
    }

    /**
     * Bootstrap a non-empty raft node.
     *
     * @param opts options of bootstrap
     * @return true if bootstrap success
     */
    public static boolean bootstrap(final BootstrapOptions opts) throws InterruptedException {
        final NodeImpl node = new NodeImpl();
        final boolean ret = node.bootstrap(opts);
        node.shutdown();
        node.join();
        return ret;
    }

    /**
     * Create a thread factory.
     *
     * @param prefixName the prefix name of thread
     * @return a new {@link ThreadFactory} instance
     *
     * @since 0.0.3
     */
    public static ThreadFactory createThreadFactory(final String prefixName) {
        return new NamedThreadFactory(prefixName, true);
    }

    /**
     * Create a configuration from a string in the form of "host1:port1[:idx],host2:port2[:idx]......",
     * returns a empty configuration when string is blank.
     */
    public static Configuration getConfiguration(final String s) {
        final Configuration conf = new Configuration();
        if (StringUtils.isBlank(s)) {
            return conf;
        }
        if (conf.parse(s)) {
            return conf;
        }
        throw new IllegalArgumentException("Invalid conf str:" + s);
    }

    /**
     * Create a peer from a string in the form of "host:port[:idx]",
     * returns a empty peer when string is blank.
     */
    public static PeerId getPeerId(final String s) {
        final PeerId peer = new PeerId();
        if (StringUtils.isBlank(s)) {
            return peer;
        }
        if (peer.parse(s)) {
            return peer;
        }
        throw new IllegalArgumentException("Invalid peer str:" + s);
    }

    /**
     * Create a Endpoint instance from  a string in the form of "host:port",
     * returns null when string is blank.
     */
    public static Endpoint getEndPoint(final String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        final String[] tmps = StringUtils.split(s, ':');
        if (tmps.length != 2) {
            throw new IllegalArgumentException("Invalid endpoint string: " + s);
        }
        return new Endpoint(tmps[0], Integer.parseInt(tmps[1]));
    }

    private JRaftUtils() {
    }
}
