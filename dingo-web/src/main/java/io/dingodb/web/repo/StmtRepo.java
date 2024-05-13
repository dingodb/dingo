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

package io.dingodb.web.repo;

import io.dingodb.web.model.dto.Stmt;
import io.dingodb.web.model.dto.StmtId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface StmtRepo extends JpaRepository<Stmt, StmtId> {

    @Query(value = "select * from information_schema.statements_summary order by exec_count desc limit 5", nativeQuery=true)
    List<Stmt> topSql();

    @Query(value = "select * from information_schema.statements_summary order by priority desc limit 100", nativeQuery=true)
    List<Stmt> topStmtSql();
}
