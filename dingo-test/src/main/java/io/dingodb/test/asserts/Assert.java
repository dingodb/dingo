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

package io.dingodb.test.asserts;

import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Vertex;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.assertj.core.api.ObjectAssert;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

public class Assert<T, A extends Assert<T, A>> {
    @Getter
    protected final T instance;

    protected Assert(T obj) {
        instance = obj;
    }

    public static <O> @Nonnull AssertObj<O> of(O obj) {
        return new AssertObj<>(obj);
    }

    @Nonnull
    public static AssertSqlNode sqlNode(SqlNode sqlNode) {
        return new AssertSqlNode(sqlNode);
    }

    @Nonnull
    public static AssertRelNode relNode(RelNode relNode) {
        return new AssertRelNode(relNode);
    }

    @Nonnull
    public static AssertJob job(Job job) {
        return new AssertJob(job);
    }

    @Nonnull
    public static AssertTask task(Task task) {
        return new AssertTask(task);
    }

    @Nonnull
    public static AssertOperator operator(Operator operator) {
        return new AssertOperator(operator);
    }

    public static AssertOperator operator(Operator operator, Vertex vertex) {
        return new AssertOperator(operator, vertex);
    }

    @Nonnull
    public static AssertResultSet resultSet(ResultSet obj) {
        return new AssertResultSet(obj);
    }

    @SuppressWarnings("UnusedReturnValue")
    public A isNull() {
        assertThat(instance).isNull();
        return cast();
    }

    public A isA(Class<? extends T> clazz) {
        assertThat(instance).isInstanceOf(clazz);
        return cast();
    }

    public A prop(String name, Object value) {
        assertThat(instance).hasFieldOrPropertyWithValue(name, value);
        return cast();
    }

    public A isEqualTo(Object value) {
        if (value == null) {
            assertThat(instance).isNull();
        } else if (instance instanceof Date || instance instanceof Time) {
            assertThat(instance.toString()).isEqualTo(value);
        } else if (instance instanceof Float) {
            assertThat((Float) instance).isCloseTo((Float) value, offset(1E-6f));
        } else if (instance instanceof Double) {
            assertThat((Double) instance).isCloseTo((Double) value, offset(1E-6));
        } else {
            assertThat(instance).isEqualTo(value);
        }
        return cast();
    }

    public ObjectAssert<T> that() {
        return assertThat(instance);
    }

    @SuppressWarnings("unchecked")
    private A cast() {
        return (A) this;
    }

    public static final class AssertObj<T> extends Assert<T, AssertObj<T>> {
        AssertObj(T obj) {
            super(obj);
        }
    }
}
