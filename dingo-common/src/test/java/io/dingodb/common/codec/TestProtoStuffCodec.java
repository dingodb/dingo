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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class TestProtoStuffCodec {

    @Test
    public void test() {
        Student stu1 = new Student(20, null, 200);
        Student stu2 = new Student(21, null, 30);
        Student stu3 = new Student(22, null, 10);
        List<Student> students = new ArrayList<Student>();
        students.add(stu1);
        students.add(null);
        students.add(stu3);
        School school = new School("abc", students);
        System.out.println("Before:==> " + school.toString());

        byte[] bytes = ProtostuffCodec.write(school);
        School deserialValue = ProtostuffCodec.read(bytes);
        System.out.println("After:==> " + deserialValue.toString());
        Assertions.assertEquals(school.toString(), deserialValue.toString());
    }
}
