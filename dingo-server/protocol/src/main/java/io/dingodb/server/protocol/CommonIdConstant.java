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

import lombok.experimental.FieldNameConstants;


public class CommonIdConstant {

    public static final IdType ID_TYPE = new IdType();
    public static final TableIdentifier TABLE_IDENTIFIER = new TableIdentifier();
    public static final ServiceIdentifier SERVICE_IDENTIFIER = new ServiceIdentifier();
    public static final DataIdentifier DATA_IDENTIFIER = new DataIdentifier();

    public static final PrivilegeIdentifier PRIVILEGE_IDENTIFIER = new PrivilegeIdentifier();

    public static final IndexIdentifier INDEX_IDENTIFIER = new IndexIdentifier();
    public static final StatsIdentifier STATS_IDENTIFIER = new StatsIdentifier();
    public static final TaskIdentifier TASK_IDENTIFIER = new TaskIdentifier();
    public static final FunctionIdentifier FUNCTION_IDENTIFIER = new FunctionIdentifier();
    public static final OtherIdentifier OTHER_IDENTIFIER = new OtherIdentifier();
    public static final int ROOT_DOMAIN = 1;
    public static final OpIdentifier OP_IDENTIFIER = new OpIdentifier();

    @FieldNameConstants(asEnum = true)
    public static final class IdType {
        public final byte table = 'T';
        public final byte service = 'A';
        public final byte stats = 'S';
        public final byte data = 'D';
        public final byte index = 'I';
        public final byte task = 'J';
        public final byte function = 'F';
        public final byte privilege = 'P';
        public final byte other = 'O';
        public final byte op = 'P';

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
        public final byte[] index = new byte[] {'T', 'I'};

        private TableIdentifier() {
        }
    }

    @FieldNameConstants
    public static final class ServiceIdentifier {
        public final byte[] executor = new byte[] {'E', 'E'};
        public final byte[] coordinator = new byte[] {'C', 'C'};
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
    public static final class PrivilegeIdentifier {
        public final byte[] user = new byte[] {'D', 'U'};
        public final byte[] schemaPrivilege = new byte[] {'D', 'S'};
        public final byte[] tablePrivilege = new byte[] {'D', 'T'};

        public final byte[] privilege = new byte[] {'D', 'I'};
        public final byte[] privilegeDict = new byte[] {'D', 'L'};

        public final byte[] privilegeType = new byte[] {'D', 'F'};

        private PrivilegeIdentifier() {
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
    public static final class FunctionIdentifier {
        public final byte[] codeUDF = new byte[] {'U', 'C'};

        private FunctionIdentifier() {
        }
    }

    @FieldNameConstants
    public static final class OtherIdentifier {
        public final byte[] other = new byte[] {'O', '0'};

        private OtherIdentifier() {
        }
    }

    @FieldNameConstants
    public static final class OpIdentifier {
        public final byte[] internal = new byte[] {'P', 'I'};
        public final byte[] external = new byte[] {'P', 'E'};

        private OpIdentifier() {
        }
    }
}
