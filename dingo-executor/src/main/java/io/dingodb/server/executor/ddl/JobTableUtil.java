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

package io.dingodb.server.executor.ddl;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dingodb.common.ddl.ActionType;
import io.dingodb.common.ddl.DdlJob;
import io.dingodb.common.ddl.DdlUtil;
import io.dingodb.common.ddl.MetaElement;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.session.Session;
import io.dingodb.common.session.SessionUtil;
import io.dingodb.common.util.Pair;
import io.dingodb.common.util.Utils;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import io.dingodb.meta.InfoSchemaService;
import lombok.extern.slf4j.Slf4j;

import java.sql.Date;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public final class JobTableUtil {
    private static final String updateDDLJobSQL = "update mysql.dingo_ddl_job set job_meta = '%s' where job_id = %d";
    private static final String getJobSQL = "select job_meta, processing, job_id from mysql.dingo_ddl_job where job_id in (select min(job_id) from mysql.dingo_ddl_job group by schema_ids, table_ids, processing) and %s reorg %s order by processing desc, job_id";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int general = 0;
    private static final int reorg = 1;

    private JobTableUtil() {
    }

    public static String updateDDLJob2Table(Session session, DdlJob ddlJob, boolean updateRawArgs) {
        byte[] bytes = ddlJob.encode(updateRawArgs);
        String jobMeta = new String(bytes);
        String sql = String.format(updateDDLJobSQL, jobMeta, ddlJob.getId());
        return session.executeUpdate(sql);
    }

    public static void cleanDDLReorgHandles(Session session, DdlJob job) {
        String sql = "delete from mysql.dingo_ddl_reorg where job_id = " + job.getId();
        session.runInTxn(session1 -> {
            session1.executeUpdate(sql);
            return null;
        });
    }

    public static String deleteDDLJob(Session session, DdlJob job) {
        String sql = "delete from mysql.dingo_ddl_job where job_id = " + job.getId();
        return session.executeUpdate(sql);
    }

    public static String addHistoryDDLJob2Table(Session session, DdlJob job, boolean updateRawArgs) {
        String time = DateTimeUtils.dateFormat(new Date(System.currentTimeMillis()), "yyyy-MM-dd HH:mm:ss");
        String sql = "insert into mysql.dingo_ddl_history(job_id, job_meta, schema_name, table_name, schema_ids, table_ids, create_time) values (%d, %s, %s, %s, %s, %s, %s)";
        sql = String.format(sql, job.getId(), Utils.quoteForSql(""), Utils.quoteForSql(job.getSchemaName()), Utils.quoteForSql(job.getTableName()), Utils.quoteForSql(job.getSchemaId()), Utils.quoteForSql(job.getTableId()), Utils.quoteForSql(time));
        session.executeUpdate(sql);
        return null;
    }

    public static void cleanMDLInfo(long jobId) {
        String sql = "delete from mysql.dingo_mdl_info where job_id = " + jobId;
        String error = SessionUtil.INSTANCE.exeUpdateInTxn(sql);
        if (error != null) {
            LogUtils.error(log, "[ddl] cleanMDLInfo error:{}, jobId:{}", error, jobId);
        }

        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        String template = DdlUtil.MDL_PREFIX_TEMPLATE;
        String key = String.format(template, DdlUtil.tenantPrefix, DdlUtil.DDLAllSchemaVersionsByJob, jobId);
        String endTemplate = DdlUtil.MDL_PREFIX_TEMPLATE_END;
        String keyEnd = String.format(endTemplate, DdlUtil.tenantPrefix, DdlUtil.DDLAllSchemaVersionsByJob, jobId);
        infoSchemaService.delKvFromCoordinator(key, keyEnd);
    }

    public static Pair<DdlJob, String> getGenerateJob(Session session) {
        try {
            return getJob(session, general, job1 -> {
                if (job1.getActionType() == ActionType.ActionDropSchema) {
                    String sql = "select job_id from mysql.dingo_ddl_job where schema_ids = %s and processing limit 1";
                    sql = String.format(sql, Utils.quoteForSql(job1.getSchemaId()));
                    return checkJobIsRunnable(session, sql);
                }
                String sql = "select job_id from mysql.dingo_ddl_job t1, (select table_ids from mysql.dingo_ddl_job where job_id = %d) t2 where " +
                    " processing and t2.table_ids = t1.table_ids";
                sql = String.format(sql, job1.getId());
                return checkJobIsRunnable(session, sql);
            });
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            return Pair.of(null, e.getMessage());
        }
    }

    public static Pair<Boolean, String> checkJobIsRunnable(Session session, String sql) {
        List<Object[]> resList;
        try {
            Timer.Context timeCtx = DingoMetrics.getTimeContext("checkJobIsRunnable");
            resList = session.executeQuery(sql);
            timeCtx.stop();
        } catch (SQLException e) {
            return Pair.of(false, e.getMessage());
        }
        return Pair.of(resList.isEmpty(), null);
    }

    public static Pair<DdlJob, String> getJob(Session session, int jobType, Function<DdlJob, Pair<Boolean, String>> filter) {
        String not = "not";
        if (jobType == 1) {
            not = "";
        }
        String sql = String.format(getJobSQL, not, DdlContext.INSTANCE.excludeJobIDs());
        try {
            long start = System.currentTimeMillis();
            List<Object[]> resList = session.executeQuery(sql);
            long cost = System.currentTimeMillis() - start;

            if (!resList.isEmpty()) {
                DingoMetrics.metricRegistry.timer("getJobSql").update(cost, TimeUnit.MILLISECONDS);
            }
            if (cost > 200) {
                LogUtils.info(log, "get job size:{}", resList.size()
                    + ", runningJobs:" + DdlContext.INSTANCE.getRunningJobs().size()
                    + ", query job sql cost:" + cost);
            }
            for (Object[] rows : resList) {
                byte[] bytes = (byte[]) rows[0];
                DdlJob ddlJob;
                try {
                    ddlJob = objectMapper.readValue(bytes, DdlJob.class);
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage(), e);
                    return Pair.of(null, e.getMessage());
                }
                boolean processing = Boolean.parseBoolean(rows[1].toString());
                if (processing) {
                    if (DdlContext.INSTANCE.getRunningJobs().containJobId(ddlJob.getId())) {
                        //LogUtils.info(log, "get job process check has running,jobId:{}", ddlJob.getId());
                        continue;
                    } else {
                        //LogUtils.info(log, "get job processing true, jobId:{}", ddlJob.getId());
                        return Pair.of(ddlJob, null);
                    }
                }
                Pair<Boolean, String> res = filter.apply(ddlJob);
                if (res.getValue() != null) {
                    return Pair.of(null, res.getValue());
                }
                if (res.getKey()) {
                    if (markJobProcessing(session, ddlJob) != null) {
                        return Pair.of(null, null);
                    }
                    return Pair.of(ddlJob, null);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Pair.of(null, null);
    }

    public static String markJobProcessing(Session session, DdlJob job) {
        Timer.Context timeCtx = DingoMetrics.getTimeContext("markJobProcessing");
        String sql = "update mysql.dingo_ddl_job set processing = true where job_id = " + job.getId();
        String res = markJobProcessing(session, sql, 3);
        timeCtx.stop();
        return res;
    }

    public static String markJobProcessing(Session session, String sql, int retry) {
        try {
            return session.executeUpdate(sql);
        } catch (Exception e) {
            LogUtils.error(log, "[ddl] mark job processing error", e);
            if (retry-- >= 0) {
                return markJobProcessing(session, sql, retry);
            }
            return e.getMessage();
        }
    }

    public static Pair<DdlJob, String> getReorgJob(Session session) {
        try {
            Timer.Context timeCtx = DingoMetrics.getTimeContext("reorgJob");
            Pair<DdlJob, String> res = getJob(session, reorg, job1 -> {
                String sql = "select job_id from mysql.dingo_ddl_job where "
                    + "(schema_ids = %s and type = %d and processing) "
                    + " or (table_ids = %s and processing) "
                    + " limit 1";
                sql = String.format(sql, Utils.quoteForSql(job1.getSchemaId()), job1.getActionType().getCode(), Utils.quoteForSql(job1.getTableId()));
                return checkJobIsRunnable(session, sql);
            });
            timeCtx.stop();
            return res;
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            return Pair.of(null, e.getMessage());
        }
    }

    public static String removeDDLReorgHandle(Session session, long jobId, MetaElement[] elements) {
        if (elements.length == 0) {
            return null;
        }
        String sqlTmp = "delete from mysql.tidb_ddl_reorg where job_id = %d";
        String sql = String.format(sqlTmp, jobId);
        try {
            session.runInTxn(t -> {
                t.executeUpdate(sql);
                return null;
            });
            return null;
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage());
            return "removeDDLReorg error";
        }
    }

}
