package io.dingodb.common.codec;

import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.TupleMapping;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class TestIndexCodec {

    @Test
    public void testCodec() throws Exception {
        ColumnDefinition cd1 = ColumnDefinition.builder().name("cd1")
            .type("STRING")
            .elementType("STRING")
            .precision(-1)
            .scale(Integer.MIN_VALUE)
            .nullable(false)
            .primary(true)
            .defaultValue("0")
            .build();

        ColumnDefinition cd2 = ColumnDefinition.builder().name("cd2")
            .type("LONG")
            .elementType("LONG")
            .precision(-1)
            .scale(Integer.MIN_VALUE)
            .nullable(false)
            .primary(false)
            .defaultValue("0")
            .build();

        ColumnDefinition cd3 = ColumnDefinition.builder().name("cd3")
            .type("STRING")
            .elementType("STRING")
            .precision(-1)
            .scale(Integer.MIN_VALUE)
            .nullable(false)
            .primary(false)
            .defaultValue("0")
            .build();

        ColumnDefinition cd4 = ColumnDefinition.builder().name("cd4")
            .type("INT")
            .elementType("INT")
            .precision(-1)
            .scale(Integer.MIN_VALUE)
            .nullable(false)
            .primary(false)
            .defaultValue("0")
            .build();

        ColumnDefinition cd5 = ColumnDefinition.builder().name("cd5")
            .type("DOUBLE")
            .elementType("DOUBLE")
            .precision(-1)
            .scale(Integer.MIN_VALUE)
            .nullable(false)
            .primary(false)
            .defaultValue("0")
            .build();

        TableDefinition td = new TableDefinition("Test");
        td.addColumn(cd1);
        td.addColumn(cd2);
        td.addColumn(cd3);
        td.addColumn(cd4);
        td.addColumn(cd5);

        Index index1 = new Index("in1", new String[]{"cd3", "cd4", "cd2"}, true);
        Index index2 = new Index("in2", new String[]{"cd3", "cd4", "cd5"}, false);

        td.addIndex(index1);
        td.addIndex(index2);

        KeyValueCodec codec = new DingoKeyValueCodec(td.getDingoType(), td.getKeyMapping());

        Map<String, TupleMapping> indexMapping = td.getIndexesMapping();
        Map<String, DingoIndexKeyValueCodec> indicsCodec = new HashMap<>();

        indexMapping.forEach((k, v) -> {
            DingoIndexKeyValueCodec indexCodec = new DingoIndexKeyValueCodec(td.getDingoType(), td.getKeyMapping(), v, td.getIndexes().get(k).isUnique());
            indicsCodec.put(k, indexCodec);
        });

        Object[] record1 = new Object[]{"1", 1L, "1", 1, 1.0};
        Object[] record2 = new Object[]{"2", 2L, "2", 2, 2.0};
        Object[] record3 = new Object[]{"3", 3L, "3", 3, 3.0};

        KeyValue r1 = codec.encode(record1);
        KeyValue i11 = indicsCodec.get("in1").encode(record1);
        KeyValue i12 = indicsCodec.get("in2").encode(record1);
        KeyValue r2 = codec.encode(record2);
        KeyValue i21 = indicsCodec.get("in1").encode(record2);
        KeyValue i22 = indicsCodec.get("in2").encode(record2);
        KeyValue r3 = codec.encode(record3);
        KeyValue i31 = indicsCodec.get("in1").encode(record3);
        KeyValue i32 = indicsCodec.get("in2").encode(record3);

        byte[] key11 = indicsCodec.get("in1").decodeKeyBytes(i11);
        byte[] key12 = indicsCodec.get("in2").decodeKeyBytes(i12);
        byte[] key21 = indicsCodec.get("in1").decodeKeyBytes(i21);
        byte[] key22 = indicsCodec.get("in2").decodeKeyBytes(i22);
        byte[] key31 = indicsCodec.get("in1").decodeKeyBytes(i31);
        byte[] key32 = indicsCodec.get("in2").decodeKeyBytes(i32);

        Assertions.assertArrayEquals(r1.getKey(), key11);
        Assertions.assertArrayEquals(r1.getKey(), key12);
        Assertions.assertArrayEquals(r2.getKey(), key21);
        Assertions.assertArrayEquals(r2.getKey(), key22);
        Assertions.assertArrayEquals(r3.getKey(), key31);
        Assertions.assertArrayEquals(r3.getKey(), key32);
    }
}
