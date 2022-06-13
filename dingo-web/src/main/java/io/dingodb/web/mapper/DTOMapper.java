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

package io.dingodb.web.mapper;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.server.protocol.meta.ExecutorStats;
import io.dingodb.server.protocol.meta.Replica;
import io.dingodb.server.protocol.meta.Schema;
import io.dingodb.server.protocol.meta.Table;
import io.dingodb.server.protocol.meta.TablePart;
import io.dingodb.server.protocol.meta.TablePartStats;
import io.dingodb.web.model.dto.meta.ExecutorDTO;
import io.dingodb.web.model.dto.meta.ExecutorStatsDTO;
import io.dingodb.web.model.dto.meta.ReplicaDTO;
import io.dingodb.web.model.dto.meta.SchemaDTO;
import io.dingodb.web.model.dto.meta.TableDTO;
import io.dingodb.web.model.dto.meta.TablePartDTO;
import io.dingodb.web.model.dto.meta.TablePartStatsDTO;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Mapper(unmappedSourcePolicy = ReportingPolicy.IGNORE, unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface DTOMapper {

    ExecutorDTO mapping(Executor executor);

    ExecutorStatsDTO mapping(ExecutorStats executorStats);

    ReplicaDTO mapping(Replica replica);

    SchemaDTO mapping(Schema schema);

    TableDTO mapping(Table table);

    TablePartDTO mapping(TablePart tablePart);

    TablePartStatsDTO mapping(TablePartStats tablePartStats);

    default String mapping(CommonId id) {
        return id.toString();
    }

    default String mapping(Location location) {
        return location.getUrl();
    }

    default int[] mapping(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        int[] result = new int[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            result[i] = bytes[i];
        }
        return result;
    }

    default byte[] mapping(int[] ints) {
        if (ints == null) {
            return null;
        }
        byte[] result = new byte[ints.length];
        for (int i = 0; i < ints.length; i++) {
            result[i] = (byte) ints[i];
        }
        return result;
    }

    default List<TablePartStatsDTO.ApproximateStats> mapping(List<TablePartStats.ApproximateStats> source) {
        if (source == null) {
            return null;
        }
        int size = source.size();
        if (size <= 3) {
            return source.stream()
                .map(stats -> merge(new TablePartStatsDTO.ApproximateStats(), stats))
                .collect(Collectors.toList());
        }
        Iterator<TablePartStats.ApproximateStats> iterator = source.iterator();
        int n = 0;
        size = size % 3 == 0 ? size / 3 : size / 3 + 1;
        List<TablePartStatsDTO.ApproximateStats> result = new ArrayList<>();
        TablePartStatsDTO.ApproximateStats stats = new TablePartStatsDTO.ApproximateStats();
        while (true) {
            if (!iterator.hasNext()) {
                if (stats.getStartKey() != null) {
                    result.add(stats);
                }
                break;
            }
            merge(stats, iterator.next());
            if (n++ % size == 0) {
                result.add(stats);
                stats = new TablePartStatsDTO.ApproximateStats();
            }
        }
        return result;
    }

    default TablePartStatsDTO.ApproximateStats merge(
        TablePartStatsDTO.ApproximateStats target, TablePartStats.ApproximateStats source
    ) {
        if (target.getStartKey() == null) {
            target.setStartKey(mapping(source.getStartKey()));
        }
        target.setEndKey(mapping(source.getEndKey()));
        target.setCount(target.getCount() + source.getCount());
        target.setSize(target.getSize() + source.getSize());
        return target;
    }

}
