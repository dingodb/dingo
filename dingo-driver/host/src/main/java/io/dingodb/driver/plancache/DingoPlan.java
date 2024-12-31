///*
// * Copyright 2021 DataCanvas
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package io.dingodb.driver.plancache;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//class PlanCacheStmt {
//    private List<Expression> params;
//    private List<TableInfo> tbls;
//    private String[] dbName;
//    private Map<Integer, Integer> RelateVersion;
//    private boolean StmtCacheable;
//    private PreparedAst preparedAst;
//    private PointGet pointGet;
//    // 其他成员变量和对应的Getter、Setter方法等根据实际需求添加
//    //...
//}
//
//// 假设这是一个表示表达式的类，对应原Go代码中的expression.Expression，需根据实际功能详细实现其方法
//class Expression {
//    // 假设的求值方法，对应原Go代码中的Eval方法，参数等需根据实际调整
//    public Datum eval(EvalContext evalCtx, Row row) {
//        // 具体求值逻辑实现
//        return null;
//    }
//    // 其他相关方法根据实际需求添加
//    //...
//}
//
//// 假设这是一个表示数据类型的类，对应原Go代码中的types.FieldType，需根据实际功能详细实现其方法和成员变量
//class FieldType {
//    // 相关属性和方法
//    //...
//}
//
//// 假设这是一个表示计划上下文的接口，对应原Go代码中的base.PlanContext，需根据实际功能详细定义其方法
//interface PlanContext {
//    SessionVars getSessionVars();
//    // 其他相关方法根据实际需求添加
//    //...
//}
//
//// 假设这是一个表示会话变量的类，对应原Go代码中的sessionctx.Context，需根据实际功能详细实现其方法和成员变量
//class SessionVars {
//    private StmtCtx stmtCtx;
//    private PlanCacheParams planCacheParams;
//    private boolean EnableNonPreparedPlanCache;
//    private boolean EnablePreparedPlanCache;
//    private Time lastUpdateTime4PC;
//    private boolean FoundInPlanCache;
//    private boolean FoundInBinding;
//    private PlanCacheValue planCacheValue;
//    // 其他成员变量和对应的Getter、Setter方法等根据实际需求添加
//    //...
//}
//
//// 假设这是一个表示语句上下文的类，对应原Go代码中的variable.StmtCtx，需根据实际功能详细实现其方法和成员变量
//class StmtCtx {
//    private int StmtType;
//    private boolean enablePlanCache;
//    private boolean skipPlanCache;
//    private String cacheType;
//    private String normalizedPlan;
//    private String planDigest;
//    private StmtHints stmtHints;
//    private Map<TableInfo, Boolean> TblInfo2UnionScan;
//    // 其他成员变量和对应的Getter、Setter方法等根据实际需求添加
//    //...
//    public void setCacheType(String cacheType) {
//        this.cacheType = cacheType;
//    }
//    public void enablePlanCache() {
//        this.enablePlanCache = true;
//    }
//    public void setSkipPlanCache(String reason) {
//        this.skipPlanCache = true;
//    }
//    public boolean useCache() {
//        return enablePlanCache &&!skipPlanCache;
//    }
//    public void setPlan(Plan plan) {
//        // 具体设置计划的逻辑
//    }
//    public void setPlanDigest(String normalizedPlan, String planDigest) {
//        this.normalizedPlan = normalizedPlan;
//        this.planDigest = planDigest;
//    }
//    public void setStmtHints(StmtHints stmtHints) {
//        this.stmtHints = stmtHints;
//    }
//}
//
//// 假设这是一个表示计划缓存参数的类，对应原Go代码中的相关功能，需根据实际功能详细实现其方法和成员变量
//class PlanCacheParams {
//    private List<Datum> paramValues;
//    public void reset() {
//        paramValues = new ArrayList<>();
//    }
//    public void append(Datum val) {
//        paramValues.add(val);
//    }
//    public List<Datum> allParamValues() {
//        return paramValues;
//    }
//    public void setForNonPrepCache(boolean isNonPrep) {
//        // 具体设置非预准备缓存相关逻辑
//    }
//}
//
//// 假设这是一个表示数据的类，对应原Go代码中的相关类型，需根据实际功能详细实现其方法和成员变量
//class Datum {
//    // 相关属性和方法，例如转换为字节数组等
//    public byte[] toBytes() {
//        return new byte[0];
//    }
//    public void setBinaryLiteral(byte[] binVal) {
//        // 设置二进制字面量的逻辑
//    }
//    public String string() {
//        return "";
//    }
//}
//
//// 假设这是一个表示计划的接口，对应原Go代码中的base.Plan，需根据实际功能详细定义其方法
//interface Plan {
//    // 例如克隆计划等相关方法，对应原Go代码中的CloneForPlanCache等功能
//    Plan cloneForPlanCache(PlanContext planCtx);
//    // 其他相关方法根据实际需求添加
//    //...
//}
//
//// 假设这是一个表示点获取计划的类，继承自Plan接口，对应原Go代码中的PointGetPlan
//class PointGetPlan implements Plan {
//    private PointGetExecutor executor;
//    private PointGetPlan fastPlan;
//    // 其他成员变量和对应的Getter、Setter方法等根据实际需求添加
//    //...
//    @Override
//    public Plan cloneForPlanCache(PlanContext planCtx) {
//        // 具体克隆逻辑实现
//        return null;
//    }
//    public void fastClonePointGetForPlanCache(PlanContext planCtx, PointGetPlan sourcePlan, PointGetPlan targetPlan) {
//        // 具体快速克隆逻辑实现
//    }
//}
//
//// 假设这是一个表示语句提示的类，对应原Go代码中的hint.StmtHints，需根据实际功能详细实现其方法和成员变量
//class StmtHints {
//    // 相关属性和方法
//    //...
//}
//
//// 假设这是一个表示表信息的类，对应原Go代码中的相关类型，需根据实际功能详细实现其方法和成员变量
//class TableInfo {
//    private int id;
//    private String name;
//    private int revision;
//    private Meta meta;
//    // 其他成员变量和对应的Getter、Setter方法等根据实际需求添加
//    //...
//    public Meta meta() {
//        return meta;
//    }
//    public int id() {
//        return id;
//    }
//    public String name() {
//        return name;
//    }
//    public int revision() {
//        return revision;
//    }
//}
//
//// 假设这是一个表示元数据的类，对应原Go代码中的相关类型，需根据实际功能详细实现其方法和成员变量
//class Meta {
//    private int id;
//    private int revision;
//    // 其他成员变量和对应的Getter、Setter方法等根据实际需求添加
//    //...
//    public int id() {
//        return id;
//    }
//    public int revision() {
//        return revision;
//    }
//}
//
//// 假设这是一个表示计划缓存值的类，对应原Go代码中的PlanCacheValue，需根据实际功能详细实现其方法和成员变量
//class PlanCacheValue {
//    private Plan plan;
//    private List<FieldName> outputColumns;
//    private StmtHints stmtHints;
//    // 其他成员变量和对应的Getter、Setter方法等根据实际需求添加
//    //...
//    public PlanCacheValue(SessionVars sctx, PlanCacheStmt stmt, String cacheKey, String binding, Plan p, List<FieldName> names, List<FieldType> paramTypes, StmtHints stmtHints) {
//        this.plan = p;
//        this.outputColumns = names;
//        this.stmtHints = stmtHints;
//    }
//}
//
//// 假设这是一个表示字段名的类，对应原Go代码中的types.FieldName，需根据实际功能详细实现其方法和成员变量
//class FieldName {
//    // 相关属性和方法
//    //...
//}
//
//// 假设这是一个表示行数据的类，对应原Go代码中的chunk.Row，需根据实际功能详细实现其方法和成员变量
//class Row {
//    // 相关属性和方法
//    //...
//}
//
//// 假设这是一个表示求值上下文的类，对应原Go代码中的相关功能，需根据实际功能详细实现其方法和成员变量
//class EvalContext {
//    private EvalCtx getEvalCtx();
//    // 其他相关方法根据实际需求添加
//    //...
//}
//public class DingoPlan {
//
//
//
//    // 对应原Go代码中的SetParameterValuesIntoSCtx方法
//    public static Error setParameterValuesIntoSCtx(PlanContext sctx, boolean isNonPrep, List<Expression> params, List<Expression> markers) {
//        SessionVars vars = sctx.getSessionVars();
//        vars.getPlanCacheParams().reset();
//        for (int i = 0; i < params.size(); i++) {
//            Expression usingParam = params.get(i);
//            Datum val;
//            try {
//                val = usingParam.eval(sctx.getExprCtx().getEvalCtx(), new Row());
//            } catch (Exception e) {
//                return new Error(e);
//            }
//            if (isGetVarBinaryLiteral(sctx, usingParam)) {
//                try {
//                    byte[] binVal = val.toBytes();
//                    val.setBinaryLiteral(binVal);
//                } catch (Exception e) {
//                    return new Error(e);
//                }
//            }
//            if (markers!= null) {
//                // 这里假设ParamMarkerExpr有对应的Java实现类，并且可以进行类似的类型转换和赋值操作，需根据实际调整
//                ParamMarkerExpr param = (ParamMarkerExpr) markers.get(i);
//                param.setDatum(val);
//                param.setInExecute(true);
//            }
//            vars.getPlanCacheParams().append(val);
//        }
//        if (vars.getStmtCtx().enableOptimizerDebugTrace && vars.getPlanCacheParams().allParamValues().size() > 0) {
//            List<Datum> vals = vars.getPlanCacheParams().allParamValues();
//            List<String> valStrs = new ArrayList<>();
//            for (Datum val : vals) {
//                valStrs.add(val.string());
//            }
//            debugtrace.recordAnyValuesWithNames(sctx, "Parameter datums for EXECUTE", valStrs);
//        }
//        vars.getPlanCacheParams().setForNonPrepCache(isNonPrep);
//        return null;
//    }
//
//    // 对应原Go代码中的planCachePreprocess方法，这里简化了一些逻辑示意，实际需要更详细处理
//    public static Error planCachePreprocess(Context ctx, SessionContext sctx, boolean isNonPrepared, InfoSchema is, PlanCacheStmt stmt, List<Expression> params) {
//        SessionVars vars = sctx.getSessionVars();
//        PreparedAst stmtAst = stmt.getPreparedAst();
//        vars.getStmtCtx().setStmtType(stmtAst.getStmtType());
//
//        // step 1: 检查参数数量
//        if (stmt.getParams().size()!= params.size()) {
//            return new Error(plannererrors.ErrWrongParamCount);
//        }
//
//        // step 2: 设置参数值
//        Error err = setParameterValuesIntoSCtx(sctx.getPlanCtx(), isNonPrepared, stmt.getParams(), stmt.getParams());
//        if (err!= null) {
//            return err;
//        }
//
//        // step 3: 添加元数据锁并检查每个表的模式版本，这里简化示意，实际逻辑复杂得多
//        boolean schemaNotMatch = false;
//        for (int i = 0; i < stmt.getDbName().length; i++) {
//            TableInfo tbl = is.tableByID(ctx, stmt.getTbls().get(i).meta().id());
//            if (tbl == null) {
//                TableInfo tblByName;
//                try {
//                    tblByName = is.tableByName(ctx, stmt.getDbName()[i], stmt.getTbls().get(i).meta().name());
//                } catch (Exception e) {
//                    return new Error(plannererrors.ErrSchemaChanged, "Schema change caused error: " + e.getMessage());
//                }
//                stmt.getRelateVersion().remove(stmt.getTbls().get(i).meta().id());
//                stmt.getTbls().set(i, tblByName);
//                stmt.getRelateVersion().put(tblByName.meta().id(), tblByName.meta().revision());
//            }
//            TableInfo newTbl;
//            try {
//                newTbl = tryLockMDLAndUpdateSchemaIfNecessary(ctx, sctx.getPlanCtx(), stmt.getDbName()[i], stmt.getTbls().get(i), is);
//            } catch (Exception e) {
//                schemaNotMatch = true;
//                continue;
//            }
//            if (stmt.getTbls().get(i).meta().revision()!= newTbl.meta().revision() || (tbl!= null && tbl.meta().revision()!= newTbl.meta().revision())) {
//                schemaNotMatch = true;
//            }
//            stmt.getTbls().set(i, newTbl);
//            stmt.getRelateVersion().put(newTbl.meta().id(), newTbl.meta().revision());
//        }
//
//        // step 4: 检查模式版本，简化示意，实际逻辑更复杂
//        if (schemaNotMatch || stmt.getSchemaVersion()!= is.schemaMetaVersion()) {
//            stmt.getPointGet().setExecutor(null);
//            stmt.getPointGet().setColumnInfos(null);
//            PreprocessorReturn ret = new PreprocessorReturn(is);
//            NodeW nodeW = resolve.newNodeW(stmtAst.getStmt());
//            try {
//                preprocess(ctx, sctx, nodeW, InPrepare, WithPreprocessorReturn(ret));
//            } catch (Exception e) {
//                return new Error(plannererrors.ErrSchemaChanged, "Schema change caused error: " + e.getMessage());
//            }
//            stmt.setResolveCtx(nodeW.getResolveContext());
//            stmt.setSchemaVersion(is.schemaMetaVersion());
//        }
//
//        // step 5: 处理过期，简化示意，实际逻辑更复杂
//        ExpiredTimeStamp4PC expiredTimeStamp4PC = domain.getDomain(sctx).expiredTimeStamp4PC();
//        if (stmt.isStmtCacheable() && expiredTimeStamp4PC.compareTo(vars.getLastUpdateTime4PC()) > 0) {
//            sctx.getSessionPlanCache().deleteAll();
//            vars.setLastUpdateTime4PC(expiredTimeStamp4PC);
//        }
//
//        // step 6: 初始化表信息到联合扫描的映射，简化示意，实际逻辑更复杂
//        for (TableInfo tbl : stmt.getTbls()) {
//            if (tableHasDirtyContent(sctx.getPlanCtx(), tbl.meta())) {
//                vars.getStmtCtx().getTblInfo2UnionScan().put(tbl.meta(), true);
//            }
//        }
//
//        return null;
//    }
//
//    // 对应原Go代码中的GetPlanFromPlanCache方法，简化示意，实际需要处理更多细节和错误情况
//    public static Plan getPlanFromPlanCache(Context ctx, SessionContext sctx, boolean isNonPrepared, InfoSchema is, PlanCacheStmt stmt, List<Expression> params) {
//        try {
//            planCachePreprocess(ctx, sctx, isNonPrepared, is, stmt, params);
//        } catch (Exception e) {
//            return null;
//        }
//
//        SessionVars sessVars = sctx.getSessionVars();
//        StmtCtx stmtCtx = sessVars.StmtCtx;
//        boolean cacheEnabled = false;
//        if (isNonPrepared) {
//            stmtCtx.SetCacheType("SessionNonPrepared");
//            cacheEnabled = sessVars.EnableNonPreparedPlanCache;
//        } else {
//            stmtCtx.SetCacheType("SessionPrepared");
//            cacheEnabled = sessVars.EnablePreparedPlanCache;
//        }
//        if (stmt.StmtCacheable && cacheEnabled) {
//            stmtCtx.EnablePlanCache();
//        }
//        if (!stmt.UncacheableReason.isEmpty()) {
//            stmtCtx.WarnSkipPlanCache(stmt.UncacheableReason);
//        }
//
//        String cacheKey = null;
//        String binding = null;
//        boolean cacheable = false;
//        String reason = null;
//        if (stmtCtx.UseCache()) {
//            try {
//                PlanCacheKey keyObj = newPlanCacheKey(sctx, stmt);
//                cacheKey = keyObj.getCacheKey();  // 假设PlanCacheKey类有获取缓存键的方法，这里需根据实际实现调整
//                binding = keyObj.getBinding();  // 假设PlanCacheKey类有获取绑定相关信息的方法，这里需根据实际实现调整
//                cacheable = keyObj.isCacheable();  // 假设PlanCacheKey类有判断是否可缓存的方法，这里需根据实际实现调整
//                reason = keyObj.getReason();  // 假设PlanCacheKey类有获取原因相关信息的方法，这里需根据实际实现调整
//                if (!cacheable) {
//                    stmtCtx.SetSkipPlanCache(reason);
//                }
//            } catch (Exception e) {
//                return null;
//            }
//        }
//
//        List<FieldType> paramTypes = parseParamTypes(sctx, params);
//        if (stmtCtx.UseCache()) {
//            PlanCacheLookupResult lookupResult = lookupPlanCache(ctx, sctx, cacheKey, paramTypes);
//            Plan plan = lookupResult.plan;
//            List<FieldName> outputCols = lookupResult.outputCols;
//            StmtHints stmtHints = lookupResult.stmtHints;
//            boolean hit = lookupResult.hit;
//            boolean skipPrivCheck = stmt.PointGet.Executor!= null;
//            if (hit && instancePlanCacheEnabled(ctx)) {
//                Plan clonedPlan = clonePlanForInstancePlanCache(ctx, sctx, stmt, plan);
//                if (clonedPlan!= null) {
//                    plan = clonedPlan;
//                }
//            }
//            if (hit) {
//                Plan adjustedPlan = adjustCachedPlan(ctx, sctx, plan, stmtHints, isNonPrepared, skipPrivCheck, binding, is, stmt);
//                if (adjustedPlan!= null) {
//                    plan = adjustedPlan;
//                    return plan, outputCols, null;
//                }
//            }
//        }
//
//        return generateNewPlan(ctx, sctx, isNonPrepared, is, stmt, cacheKey, binding, paramTypes);
//    }
//
//    // 对应clonePlanForInstancePlanCache方法
//    public static Plan clonePlanForInstancePlanCache(Context ctx, SessionContext sctx, PlanCacheStmt stmt, Plan plan) {
//        long startTime = System.currentTimeMillis();
//        boolean fastPoint = stmt.pointGet.Executor!= null;
//        boolean isPoint = plan instanceof PointGetPlan;
//        PointGetPlan pointPlan = null;
//        if (isPoint) {
//            pointPlan = (PointGetPlan) plan;
//        }
//        Plan clonedPlan = null;
//        boolean ok = false;
//        if (fastPoint && isPoint) {
//            if (stmt.pointGet.FastPlan == null) {
//                stmt.pointGet.FastPlan = new PointGetPlan();
//            }
//            // 假设FastClonePointGetForPlanCache方法已经有对应的Java实现逻辑
//            FastClonePointGetForPlanCache(sctx.getPlanCtx(), pointPlan, stmt.pointGet.FastPlan);
//            clonedPlan = stmt.pointGet.FastPlan;
//        } else {
//            try {
//                clonedPlan = plan.cloneForPlanCache(sctx.getPlanCtx());
//                ok = true;
//            } catch (Exception e) {
//                return null;
//            }
//        }
//        if (InTest && ctx.getValue(PlanCacheKeyTestClone.class)!= null) {
//            Function<Plan, Plan, Void> func = (Function<Plan, Plan, Void>) ctx.getValue(PlanCacheKeyTestClone.class);
//            func.apply(plan, clonedPlan);
//        }
//        long endTime = System.currentTimeMillis();
//        if (ok) {
//            // 假设GetPlanCacheCloneDuration是用于记录克隆时长的相关方法，这里需按实际完善其逻辑
//            GetPlanCacheCloneDuration().observe((endTime - startTime) / 1000.0);
//        }
//        return clonedPlan;
//    }
//
//    // 对应instancePlanCacheEnabled方法
//    public static boolean instancePlanCacheEnabled(Context ctx) {
//        if (InTest && ctx.getValue(PlanCacheKeyEnableInstancePlanCache.class)!= null) {
//            return true;
//        }
//        // 假设EnableInstancePlanCache是一个用于获取是否启用实例计划缓存的配置类或变量，这里需按实际完善其获取逻辑
//        boolean enableInstancePlanCache = EnableInstancePlanCache.load();
//        return enableInstancePlanCache;
//    }
//
//    // 对应lookupPlanCache方法
//    public static LookupResult lookupPlanCache(Context ctx, SessionContext sctx, String cacheKey, List<FieldType> paramTypes) {
//        boolean useInstanceCache = instancePlanCacheEnabled(ctx);
//        long startTime = System.currentTimeMillis();
//        Object v = null;
//        boolean hit = false;
//        if (useInstanceCache) {
//            // 假设GetInstancePlanCache是Domain类（这里未完整定义）中的方法用于获取实例计划缓存实例，并且其Get方法按实际逻辑实现
//            v = domain.GetDomain(sctx).GetInstancePlanCache().Get(cacheKey, paramTypes);
//            hit = v!= null;
//        } else {
//            // 假设GetSessionPlanCache是SessionContext类中获取会话计划缓存的方法，其Get方法按实际逻辑实现
//            v = sctx.getSessionPlanCache().Get(cacheKey, paramTypes);
//            hit = v!= null;
//        }
//        if (!hit) {
//            return new LookupResult(null, null, null, false);
//        }
//        PlanCacheValue pcv = (PlanCacheValue) v;
//        sctx.getSessionVars().PlanCacheValue = pcv;
//        long endTime = System.currentTimeMillis();
//        if (hit) {
//            // 假设GetPlanCacheLookupDuration是用于记录查找时长的相关方法，按实际完善其逻辑
//            GetPlanCacheLookupDuration(useInstanceCache).observe((endTime - startTime) / 1000.0);
//        }
//        return new LookupResult(pcv.Plan, pcv.OutputColumns, pcv.StmtHints, true);
//    }
//
//    // 自定义一个用于封装查找计划缓存结果的类
//    class LookupResult {
//        Plan plan;
//        NameSlice outputCols;
//        StmtHints stmtHints;
//        boolean hit;
//        public LookupResult(Plan plan, NameSlice outputCols, StmtHints stmtHints, boolean hit) {
//            this.plan = plan;
//            this.outputCols = outputCols;
//            this.stmtHints = stmtHints;
//            this.hit = hit;
//        }
//    }
//
//    // 对应adjustCachedPlan方法
//    public static Plan adjustCachedPlan(Context ctx, SessionContext sctx, Plan plan, StmtHints stmtHints, boolean isNonPrepared, boolean skipPrivCheck, String bindSQL, InfoSchema is, PlanCacheStmt stmt) throws Exception {
//        SessionVars sessVars = sctx.getSessionVars();
//        StmtCtx stmtCtx = sessVars.StmtCtx;
//        if (!skipPrivCheck) {
//            Exception err = checkPreparedPriv(ctx, sctx, stmt, is);
//            if (err!= null) {
//                return null;
//            }
//        }
//        if (!RebuildPlan4CachedPlan(plan)) {
//            return null;
//        }
//        sessVars.FoundInPlanCache = true;
//        if (bindSQL!= null && bindSQL.length() > 0) {
//            sessVars.FoundInBinding = true;
//        }
//        if (ResettablePlanCacheCounterFortTest) {
//            PlanCacheCounter.WithLabelValues("prepare").inc();
//        } else {
//            GetPlanCacheHitCounter(isNonPrepared).inc();
//        }
//        stmtCtx.setPlanDigest(stmt.NormalizedPlan, stmt.PlanDigest);
//        stmtCtx.setStmtHints(stmtHints);
//        return plan;
//    }
//    // 对应generateNewPlan方法
//    public static PlanAndNames generateNewPlan(Context ctx, SessionContext sctx, boolean isNonPrepared, InfoSchema is, PlanCacheStmt stmt, String cacheKey, String binding, List<FieldType> paramTypes) throws Exception {
//        PreparedAst stmtAst = stmt.preparedAst;
//        SessionVars sessVars = sctx.getSessionVars();
//        StmtCtx stmtCtx = sessVars.StmtCtx;
//
//        // 假设GetPlanCacheMissCounter是用于记录计划缓存未命中次数的相关方法，按实际完善其逻辑
//        GetPlanCacheMissCounter(isNonPrepared).inc();
//        sessVars.StmtCtx.InPreparedPlanBuilding = true;
//        NodeW nodeW = resolve.newNodeWWithCtx(stmtAst.Stmt, stmt.ResolveCtx);
//        Plan plan = null;
//        List<FieldName> names = null;
//        try {
//            // 假设OptimizeAstNode是执行优化节点操作的方法，按实际完善其逻辑
//            PlanAndNames result = OptimizeAstNode(ctx, sctx, nodeW, is);
//            plan = result.plan;
//            names = result.names;
//        } catch (Exception e) {
//            sessVars.StmtCtx.InPreparedPlanBuilding = false;
//            throw e;
//        }
//        sessVars.StmtCtx.InPreparedPlanBuilding = false;
//
//        // 检查计划是否可缓存
//        if (stmtCtx.useCache()) {
//            boolean cacheable = isPlanCacheable(sctx.getPlanCtx(), plan, paramTypes.size(), stmt.limits.size(), stmt.hasSubquery);
//            if (!cacheable) {
//                stmtCtx.setSkipPlanCache("具体不可缓存原因需按实际补充");
//            }
//        }
//
//        // 将计划放入计划缓存
//        if (stmtCtx.useCache()) {
//            PlanAndDigest planAndDigest = NormalizePlan(plan);
//            stmt.NormalizedPlan = planAndDigest.normalizedPlan;
//            stmt.PlanDigest = planAndDigest.planDigest;
//            PlanCacheValue cached = new PlanCacheValue(sctx, stmt, cacheKey, binding, plan, names, paramTypes, stmtCtx.stmtHints);
//            stmtCtx.setPlan(plan);
//            stmtCtx.setPlanDigest(stmt.NormalizedPlan, stmt.PlanDigest);
//            if (instancePlanCacheEnabled(ctx)) {
//                domain.GetDomain(sctx).GetInstancePlanCache().put(cacheKey, cached, paramTypes);
//            } else {
//                sctx.getSessionPlanCache().put(cacheKey, cached, paramTypes);
//            }
//            sctx.getSessionVars().PlanCacheValue = cached;
//        }
//        sessVars.FoundInPlanCache = false;
//        return new PlanAndNames(plan, names);
//    }
//
//    // 自定义一个用于封装计划和字段名的类
//    class PlanAndNames {
//        Plan plan;
//        List<FieldName> names;
//        public PlanAndNames(Plan plan, List<FieldName> names) {
//            this.plan = plan;
//            this.names = names;
//        }
//    }
//
//    // 自定义一个用于封装计划和计划摘要的类
//    class PlanAndDigest {
//        String normalizedPlan;
//        String planDigest;
//        public PlanAndDigest(String normalizedPlan, String planDigest) {
//            this.normalizedPlan = normalizedPlan;
//            this.planDigest = planDigest;
//        }
//    }
//
//    // 对应checkPreparedPriv方法
//    public static Error checkPreparedPriv(Context ctx, SessionContext sctx, PlanCacheStmt stmt, InfoSchema is) throws Exception {
//        PrivilegeManager pm = privilege.getPrivilegeManager(sctx);
//        if (pm!= null) {
//            VisitInfo visitInfo = VisitInfo4PrivCheck(ctx, is, stmt.preparedAst.Stmt, stmt.VisitInfos);
//            try {
//                // 假设CheckPrivilege是执行权限检查的具体方法，按实际完善其逻辑
//                CheckPrivilege(sctx.getSessionVars().ActiveRoles, pm, visitInfo);
//            } catch (Exception e) {
//                return new Error(e);
//            }
//        }
//        // 假设CheckTableLock是检查表锁的具体方法，按实际完善其逻辑
//        return CheckTableLock(sctx, is, stmt.VisitInfos);
//    }
//
//    // 对应IsSafeToReusePointGetExecutor方法
//    public static boolean isSafeToReusePointGetExecutor(SessionContext sctx, InfoSchema is, PlanCacheStmt stmt) {
//        if (staleread.isStmtStaleness(sctx)) {
//            return false;
//        }
//        // 假设IsAutoCommitTxn是检查是否自动提交事务的方法，按实际完善其逻辑
//        if (!IsAutoCommitTxn(sctx.getSessionVars())) {
//            return false;
//        }
//        if (stmt.SchemaVersion!= is.SchemaMetaVersion()) {
//            return false;
//        }
//        return true;
//    }
//
//}
