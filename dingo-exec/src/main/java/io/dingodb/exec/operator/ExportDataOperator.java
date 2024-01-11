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

package io.dingodb.exec.operator;

import com.google.common.primitives.Bytes;
import io.dingodb.common.exception.DingoSqlException;
import io.dingodb.common.mysql.MysqlByteUtil;
import io.dingodb.common.util.ByteUtils;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.params.ExportDataParam;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.common.mysql.constant.ServerConstant.ARRAY_SPLIT;
import static io.dingodb.common.mysql.util.DataTimeUtils.getTimeStamp;

@Slf4j
public class ExportDataOperator extends SoleOutOperator {
    public static final ExportDataOperator INSTANCE = new ExportDataOperator();
    private final String WRITE_FILE_ERROR = "Error 1 (HY000): Can't create/write to file '%s' "
        + "(Errcode: 13 - Permission denied)";
    private final String FILE_EXISTS = "Error 1086(HY000): File '%s' already exists";
    private final byte[] EMPTY_BYTES = "\\N".getBytes();

    private final Map<String, FileOutputStream> fileMap = new ConcurrentHashMap<>();

    @Override
    public boolean push(int pin, @Nullable Object[] tuple, Vertex vertex) {
        ExportDataParam param = vertex.getParam();
        writeFiles(tuple, param);
        return true;
    }

    public void writeFiles(Object[] tuple, ExportDataParam param) {
        byte[] terminated = param.getTerminated();
        byte[] enclosed = param.getEnclosed();
        byte[] lineTerminated = param.getLineTerminated();
        byte[] lineStarting = param.getLineStarting();
        String charset = param.getCharset();
        List<Byte> escaped = new ArrayList<>();
        escaped.addAll(Bytes.asList(param.getEscaped()));

        FileOutputStream writer = fileMap.get(param.getId());
        try {
            if (writer == null) {
                File file = new File(param.getOutfile());
                if (!file.exists()) {
                    file.createNewFile();
                } else {
                    throw new DingoSqlException(String.format(FILE_EXISTS, param.getOutfile()));
                }
                writer = new FileOutputStream(param.getOutfile());
                fileMap.put(param.getId(), writer);
            }

            int tupleLength = tuple.length;
            int tupleLimitLen = tupleLength - 1;
            if (lineStarting != null) {
                writer.write(lineStarting);
            }
            for (int j = 0; j < tupleLength; j++) {
                Object val = tuple[j];
                if (enclosed != null) {
                    writer.write(enclosed);
                }
                if (val == null) {
                    writer.write(EMPTY_BYTES);
                } else if (val instanceof byte[]) {
                    byte[] bytes = (byte[]) val;
                    String base64String = Base64.getEncoder().encodeToString(bytes);
                    writer.write(base64String.getBytes());
                } else if (val instanceof Timestamp) {
                    writer.write(getTimeStamp((Timestamp) val).getBytes());
                } else if (val instanceof Boolean) {
                    boolean valBool = (boolean) val;
                    if (valBool) {
                        writer.write(49);
                    } else {
                        writer.write(48);
                    }
                } else if (val instanceof ArrayList) {
                    List<Object> list = (List<Object>) val;
                    writer.write("[".getBytes());
                    StringBuilder line = new StringBuilder();
                    int len = list.size();
                    int limitLen = len - 1;
                    for (int i = 0; i < len; i++) {
                        line.append(list.get(i));
                        if (i < limitLen) {
                            line.append(ARRAY_SPLIT);
                        }
                    }
                    writer.write(line.toString().getBytes(charset));
                    writer.write("]".getBytes());
                } else if (val instanceof String) {
                    byte[] bytes = val.toString().getBytes();
                    bytes = escaped(bytes, terminated, lineTerminated, escaped);
                    writer.write(bytes);
                } else if (val instanceof LinkedHashMap) {
                    byte[] bytes = val.toString().getBytes(charset);
                    bytes = escaped(bytes, terminated, lineTerminated, escaped);
                    writer.write(bytes);
                } else {
                    writer.write(val.toString().getBytes(charset));
                }
                if (enclosed != null) {
                    writer.write(enclosed);
                }
                if (j < tupleLimitLen) {
                    writer.write(terminated);
                }
            }
            writer.write(lineTerminated);
        } catch (IOException e) {
            throw new DingoSqlException(String.format(WRITE_FILE_ERROR, param.getOutfile()));
        }
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        Edge edge = vertex.getSoleEdge();
        try {
            ExportDataParam param = vertex.getParam();
            FileOutputStream writer = fileMap.get(param.getId());
            if (writer != null) {
                writer.close();
                fileMap.remove(param.getId());
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            edge.fin(fin);
        }
    }

    private byte[] escaped(byte[] source, byte[] fieldTerm, byte[] lineTerm, List<Byte> escaped) {
        List<Byte> bytes =  new ArrayList<>();
        int len = source.length;
        boolean term1 = fieldTerm.length == 1;
        boolean term2 = fieldTerm.length == 2;

        boolean lterm1 = lineTerm.length == 1;
        boolean lterm2 = lineTerm.length == 2;

        for (int i = 0; i < len; i ++) {
            byte b = source[i];
            if (term1 && b == fieldTerm[0]) {
                bytes.addAll(escaped);
            } else if (term2 && b == fieldTerm[1] && i >= 1 && source[i - 1] == fieldTerm[0]) {
                bytes.addAll(escaped);
            } else if (lterm1 && b == lineTerm[0]) {
                bytes.addAll(escaped);
            } else if (lterm2 && b == lineTerm[1] && i >= 1 && source[i - 1] == lineTerm[0]) {
                bytes.addAll(escaped);
            }
            bytes.add(b);
        }

        return Bytes.toArray(bytes);
    }
}
