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

package io.dingodb.server.protocol;

import io.dingodb.common.codec.PrimitiveCodec;
import lombok.experimental.FieldNameConstants;


public class CommonIdConstant {

    public static final IdType ID_TYPE = new IdType();
    public static final TableIdentifier TABLE_IDENTIFIER = new TableIdentifier();
    public static final ServiceIdentifier SERVICE_IDENTIFIER = new ServiceIdentifier();
    public static final DataIdentifier DATA_IDENTIFIER = new DataIdentifier();
    public static final IndexIdentifier INDEX_IDENTIFIER = new IndexIdentifier();
    public static final StatsIdentifier STATS_IDENTIFIER = new StatsIdentifier();
    public static final TaskIdentifier TASK_IDENTIFIER = new TaskIdentifier();
    public static final OtherIdentifier OTHER_IDENTIFIER = new OtherIdentifier();
    public static final byte[] ZERO_DOMAIN = PrimitiveCodec.encodeInt(0);

    @FieldNameConstants(asEnum = true)
    public static final class IdType {
        public final byte table = 'T';
        public final byte service = 'A';
        public final byte stats = 'S';
        public final byte data = 'D';
        public final byte index = 'I';
        public final byte task = '0';
        public final byte other = 'O';

        private IdType() {
        }
    }

    @FieldNameConstants
    public static final class TableIdentifier {
        public final byte[] table = new byte[] {'T', 'B'};
        public final byte[] schema = new byte[] {'T', 'S'};
        public final byte[] column = new byte[] {'T', 'C'};
        public final byte[] part = new byte[] {'D', 'P'};
        public final byte[] replica = new byte[] {'D', 'R'};

        private TableIdentifier() {
        }
    }

    @FieldNameConstants
    public static final class ServiceIdentifier {
        public final byte[] executor = new byte[] {'E', 'E'};
        public final byte[] driverProxy = new byte[] {'D', 'P'};

        private ServiceIdentifier() {
        }
    }

    @FieldNameConstants
    public static final class StatsIdentifier {
        public final byte[] executor = new byte[] {'S', 'E'};
        public final byte[] replica = new byte[] {'S', 'R'};
        public final byte[] part = new byte[] {'S', 'P'};

        private StatsIdentifier() {
        }
    }

    @FieldNameConstants
    public static final class DataIdentifier {
        public final byte[] data = new byte[] {'D', 'D'};
        public final byte[] seq = new byte[] {'S', 'Q'};

        private DataIdentifier() {
        }
    }

    @FieldNameConstants
    public static final class IndexIdentifier {
        public final byte[] data = new byte[] {'I', 'D'};
        public final byte[] replicaExecutor = new byte[] {'I', 'R'};

        private IndexIdentifier() {
        }
    }

    @FieldNameConstants
    public static final class TaskIdentifier {
        public final byte[] split = new byte[] {'K', 'S'};

        private TaskIdentifier() {
        }
    }

    @FieldNameConstants
    public static final class OtherIdentifier {
        public final byte[] other = new byte[] {'O', '0'};

        private OtherIdentifier() {
        }
    }
}
