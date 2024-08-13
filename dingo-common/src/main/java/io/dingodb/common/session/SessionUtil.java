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

package io.dingodb.common.session;

import io.dingodb.common.log.LogUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Data
@Slf4j
public final class SessionUtil {
    public static final SessionUtil INSTANCE = new SessionUtil();

    public Map<String, Connection> connectionMap = new ConcurrentHashMap<>();

    private SessionPool sessionPool;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private SessionUtil() {
    }

    public synchronized void initPool() {
        if (this.sessionPool != null) {
            return;
        }
        SessionFactory sessionFactory = new SessionFactory();
        GenericObjectPoolConfig<Session> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(10000);
        config.setMinIdle(10);
        config.setMaxWaitMillis(120000L);
        this.sessionPool = new SessionPool(sessionFactory, config);
    }

    public Session getSession() {
        try {
            //LogUtils.info(log, "[ddl] sessionPool active count: {}, borrow count:{}, return count:{}",
            //    this.sessionPool.getNumActive(), this.sessionPool.getBorrowedCount(), this.sessionPool.getReturnedCount());
            return this.sessionPool.borrowObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void closeSession(Session session) {
        if (session == null) {
            return;
        }
        this.sessionPool.returnObject(session);
    }

    public String exeUpdateInTxn(String sql) {
        return exeUpdateInTxn(sql, 1);
    }

    public String exeUpdateInTxn(String sql, int retry) {
        Session session = null;
        try {
            session = getSession();
            session.runInTxn(se -> {
                se.executeUpdate(sql);
                return null;
            });
            return null;
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage());
            retry --;
            if (retry == 0) {
                return e.getMessage();
            } else {
                return exeUpdateInTxn(sql, retry);
            }
        } finally {
            closeSession(session);
        }
    }

    public void executeUpdate(List<String> sqlList) {
        Session session = getSession();
        session.executeUpdate(sqlList);
        closeSession(session);
    }

    public void wLock() {
        this.lock.writeLock().lock();
    }

    public void wUnlock() {
        this.lock.writeLock().unlock();
    }

    public void rLock() {
        this.lock.readLock().lock();
    }

    public void rUnlock() {
        this.lock.readLock().unlock();
    }

}
