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

package io.dingodb.helix.part;

import io.dingodb.helix.HelixAccessor;
import io.dingodb.server.ServerConfiguration;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;

public interface HelixPart extends AutoCloseable {

    /**
     * Returns the helix manager.
     */
    HelixManager helixManager();

    /**
     * Returns the helix data accessor.
     */
    HelixDataAccessor dataAccessor();

    /**
     * Returns the helix config accessor.
     */
    ConfigAccessor configAccessor();

    /**
     * Returns the helix accessor, config, data, property....
     */
    HelixAccessor helixAccessor();

    /**
     * Return controller name.
     */
    String name();

    /**
     * Returns the cluster name.
     */
    String clusterName();

    /**
     * Returns the server configuration.
     */
    ServerConfiguration configuration();

    /**
     * Initialize node.
     */
    void init() throws Exception;

    /**
     * start.
     */
    void start() throws Exception;

    /**
     * Return message service.
     */
    default ClusterMessagingService messagingService() {
        return helixManager().getMessagingService();
    }

    enum ControllerMode {
        /**
         * Standalone.
         */
        STANDALONE,
        /**
         * Distributed.
         */
        DISTRIBUTED
    }

    enum StateMode {
        /**
         * Leader and standby.
         */
        LEADER_STANDBY("LeaderStandby"),
        /**
         * Table leader and replica.
         */
        LEADER_FOLLOWER("LeaderFollower"),
        /**
         * Master and slave.
         */
        MASTER_SLAVE("MasterSlave");

        public final String name;

        StateMode(String name) {
            this.name = name;
        }

        public static StateMode lookup(String name) {
            return MASTER_SLAVE.name.equalsIgnoreCase(name) ? MASTER_SLAVE :
                LEADER_STANDBY.name.equalsIgnoreCase(name) ? LEADER_STANDBY : LEADER_FOLLOWER;
        }

    }

}
