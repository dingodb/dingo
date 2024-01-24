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

package io.dingodb.common.codec;

import io.dingodb.common.codec.pojo.School;
import io.dingodb.common.codec.pojo.Student;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestProtostuffCodec {
    @BeforeAll
    public static void setupAll() {
    }

    @Test
    public void test() {
        Student stu1 = new Student(20, null, 200, null);
        Student stu2 = new Student(21, null, 30, null);
        Student stu3 = new Student(22, null, 10, null);
        List<Student> students = new ArrayList<Student>();
        students.add(stu1);
        students.add(null);
        students.add(stu3);
        School school = new School("abc", students);
        System.out.println("Before:==> " + school);

        byte[] bytes = ProtostuffCodec.write(school);
        School deserialValue = ProtostuffCodec.read(bytes);
        System.out.println("After:==> " + deserialValue.toString());
        Assertions.assertEquals(school.toString(), deserialValue.toString());
    }

    @Test
    public void testNest() {
        Student studentA = new Student(1, null, 1, null);
        Student studentB = new Student(2, null, 2, null);
        studentA.setStudent(studentB);
        studentB.setStudent(studentA);
        ProtostuffCodec.read(ProtostuffCodec.write(studentA));
    }

    @Test
    public void testDate() {
        Date obj = new Date(12345);
        byte[] bytes = ProtostuffCodec.write(obj);
        Date obj1 = ProtostuffCodec.read(bytes);
        assertThat(obj1).isEqualTo(obj);
    }

    @Test
    public void testTime() {
        Time obj = new Time(12345);
        byte[] bytes = ProtostuffCodec.write(obj);
        Time obj1 = ProtostuffCodec.read(bytes);
        assertThat(obj1).isEqualTo(obj);
    }

    @Test
    public void testTimestamp() {
        Timestamp obj = new Timestamp(12345);
        byte[] bytes = ProtostuffCodec.write(obj);
        Timestamp obj1 = ProtostuffCodec.read(bytes);
        assertThat(obj1).isEqualTo(obj);
    }
}
