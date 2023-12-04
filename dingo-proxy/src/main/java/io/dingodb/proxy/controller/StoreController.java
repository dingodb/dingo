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

package io.dingodb.proxy.controller;

import io.dingodb.client.DingoClient;
import io.dingodb.client.common.Key;
import io.dingodb.client.common.Record;
import io.dingodb.client.common.Value;
import io.dingodb.sdk.common.table.Table;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Api("Store")
@RestController
@RequestMapping("/store")
public class StoreController {

    @Autowired
    private DingoClient dingoClient;

    @ApiOperation("Put data")
    @PutMapping("/api/{schema}/{table}")
    public ResponseEntity<List<Boolean>> put(@PathVariable String schema, @PathVariable String table, List<Object[]> records) {
        Table definition = dingoClient.getTableDefinition(table);
        List<Record> recordList = records.stream().map(r -> new Record(definition.getColumns(), r)).collect(Collectors.toList());
        return ResponseEntity.ok(dingoClient.upsert(schema, table, recordList));
    }

    @ApiOperation("Get by primary keys")
    @PostMapping("/api/{schema}/{table}")
    public ResponseEntity<List<Record>> get(@PathVariable String schema, @PathVariable String table, List<Object[]> keys) {
        List<Key> keyList = keys.stream().map(k -> new Key(Arrays.stream(k).map(Value::get).collect(Collectors.toList()))).collect(Collectors.toList());
        List<Record> records = dingoClient.get(schema, table, keyList);
        return ResponseEntity.ok(records);
    }

    @ApiOperation("Delete by primary keys")
    @DeleteMapping("/api/{schema}/{table}")
    public ResponseEntity<List<Boolean>> delete(@PathVariable String schema, @PathVariable String table, List<Object[]> keys) {
        List<Key> keyList = keys.stream().map(k -> new Key(Arrays.stream(k).map(Value::get).collect(Collectors.toList()))).collect(Collectors.toList());

        return ResponseEntity.ok(dingoClient.delete(schema, table, keyList));
    }

}
