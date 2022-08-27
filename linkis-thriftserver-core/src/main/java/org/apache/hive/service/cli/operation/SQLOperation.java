package org.apache.hive.service.cli.operation;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.common.MetricsScope;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.QueryDisplay;
import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.service.cli.*;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.linkis.common.conf.Configuration;
import org.apache.linkis.common.utils.Utils;
import org.apache.linkis.thriftserver.conf.LinkisThriftServerConfiguration;
import org.apache.linkis.thriftserver.exception.LinkisThriftServerWarnException;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SQLOperation extends ExecuteStatementOperation {

    protected TableSchema resultSchema = null;
    protected Schema mResultSchema = null;
    private boolean fetchStarted = false;
    private AbstractSerDe serde = null;
    private volatile MetricsScope currentSQLStateScope;
    protected SQLOperationDisplay sqlOpDisplay;
    private long queryTimeout;
    private final boolean runAsync;
    private static final Map<String, AtomicInteger> userQueries = new HashMap<>();
    private static final String SEPARATOR = "`";
    private MetricsScope submittedQryScp;

    public SQLOperation(HiveSession parentSession, String statement, Map<String, String> confOverlay, boolean runInBackground, long queryTimeout) {
        super(parentSession, statement, confOverlay, runInBackground);
        this.runAsync = runInBackground;
        this.queryTimeout = queryTimeout;
        long timeout = LinkisThriftServerConfiguration.QUERY_TIMEOUT_SECONDS().getValue().toLong();
        if (timeout > 0L && (queryTimeout <= 0L || timeout < queryTimeout)) {
            this.queryTimeout = timeout;
        }

        try {
            this.sqlOpDisplay = new SQLOperationDisplay(this);
        } catch (HiveSQLException var10) {
            LOG.warn("Error calcluating SQL Operation Display for webui", var10);
        }

        Metrics metrics = MetricsFactory.getInstance();
        if (metrics != null) {
            this.submittedQryScp = metrics.createScope("hs2_submitted_queries");
        }

    }

    @Override
    protected void registerLoggingContext() {
    }

    @Override
    public boolean shouldRunAsync() {
        return this.runAsync;
    }

    protected void prepareAsync() {
        if (this.queryTimeout > 0L) {
            Runnable timeoutTask = () -> {
                String queryId = SQLOperation.this.confOverlay.get(ConfVars.HIVEQUERYID.varname);
                try {
                    Operation.LOG.info("The Job {} of user {} timed out after: {} seconds. Cancelling the execution now: ", queryId, parentSession.getUserName(), SQLOperation.this.queryTimeout);
                    SQLOperation.this.cancel(OperationState.TIMEDOUT);
                } catch (HiveSQLException e) {
                    Operation.LOG.error("Error cancelling the Job {} after timeout {} seconds.", queryId, SQLOperation.this.queryTimeout, e);
                }
            };
            Utils.defaultScheduler().schedule(timeoutTask, this.queryTimeout, TimeUnit.SECONDS);
        }
    }

    protected void runQuery() throws HiveSQLException {
        throw new LinkisThriftServerWarnException(80001, "runQuery is not supported.");
    }

    @Override
    public void runInternal() throws HiveSQLException {
        this.setState(OperationState.PENDING);
        LOG.info("User {} from Session({}) try to run statement {}.", parentSession.getUserName(), parentSession.getSessionHandle().getHandleIdentifier(), statement);
        this.sqlOpDisplay.setQueryDisplay(new QueryDisplay());
        sqlOpDisplay.getQueryDisplay().setQueryStr(statement);
        boolean runAsync = this.shouldRunAsync();
        if (!runAsync) {
            OperationState opState = this.getStatus().getState();
            if (opState.isTerminal()) {
                LOG.info("Not running the statement. Operation is already in terminal state: " + opState + ", perhaps cancelled due to query timeout or by another thread.");
                return;
            }
            this.setState(OperationState.RUNNING);
            try {
                this.runQuery();
            } catch (Exception e) {
                this.setState(OperationState.ERROR);
                throw new HiveSQLException("execute statement failed. Reason: " + ExceptionUtils.getRootCauseMessage(e), e);
            }
            this.setState(OperationState.FINISHED);
        } else {
            SQLOperation.BackgroundWork work = new SQLOperation.BackgroundWork();
            try {
                Future<?> backgroundHandle = this.getParentSession().submitBackgroundOperation(work);
                this.setBackgroundHandle(backgroundHandle);
            } catch (RejectedExecutionException e) {
                this.setState(OperationState.ERROR);
                throw new HiveSQLException("The background threadpool cannot accept new task for execution, please retry the operation", e);
            }
        }

    }

    private void registerCurrentOperationLog() {
        if (this.isOperationLogEnabled) {
            if (this.operationLog == null) {
                LOG.warn("Failed to get current OperationLog object of Operation: " + this.getHandle().getHandleIdentifier());
                this.isOperationLogEnabled = false;
                return;
            }

            OperationLog.setCurrentOperationLog(this.operationLog);
        }

    }

    private synchronized void cleanup(OperationState state) throws HiveSQLException {
        this.setState(state);
        if (this.shouldRunAsync()) {
            Future<?> backgroundHandle = this.getBackgroundHandle();
            if (backgroundHandle != null) {
                killJob();
                boolean success = backgroundHandle.cancel(true);
                if (success) {
                    String queryId = this.confOverlay.get(ConfVars.HIVEQUERYID.varname);
                    LOG.info("The running Job {} of user {} has been successfully interrupted.", queryId, parentSession.getUserName());
                }
            }
        }
    }

    protected void killJob() {

    }

    @Override
    public void cancel(OperationState stateAfterCancel) throws HiveSQLException {
        this.cleanup(stateAfterCancel);
        this.cleanupOperationLog();
    }

    @Override
    public void close() throws HiveSQLException {
        this.cleanup(OperationState.CLOSED);
        this.cleanupOperationLog();
    }

    @Override
    public TableSchema getResultSetSchema() throws HiveSQLException {
        this.assertState(Collections.singletonList(OperationState.FINISHED));
        if (this.resultSchema == null) {
            throw new HiveSQLException("resultSchema is not ready.");
        }
        return this.resultSchema;
    }

    /**
     * 重置到第一行结果集
     */
    protected void resetFetch() {
        throw new LinkisThriftServerWarnException(80001, "resetFetch is not supported.");
    }

    protected List<List<Object>> fetchNext(int maxRows) {
        throw new LinkisThriftServerWarnException(80001, "fetchNext is not supported.");
    }

    @Override
    public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
        this.validateDefaultFetchOrientation(orientation);
        this.assertState(Collections.singletonList(OperationState.FINISHED));

        RowSet rowSet = RowSetFactory.create(this.resultSchema, this.getProtocolVersion(), false);
        try {
            if (orientation.equals(FetchOrientation.FETCH_FIRST) && this.fetchStarted) {
                resetFetch();
            }
            this.fetchStarted = true;
            List<List<Object>> convey = fetchNext((int) maxRows);
            if (CollectionUtils.isNotEmpty(convey)) {
                return this.decode(convey, rowSet);
            }
        } catch (Exception e) {
            throw new HiveSQLException(e);
        }
        return rowSet;
    }

    @Override
    public String getTaskStatus() throws HiveSQLException {
        return null;
    }

    private RowSet decode(List<List<Object>> rows, RowSet rowSet) throws SQLException, SerDeException {
        this.initSerDe();
        StructObjectInspector soi = (StructObjectInspector) this.serde.getObjectInspector();
        List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();
        Object[] deserializedFields = new Object[fieldRefs.size()];
        int protocol = this.getProtocolVersion().getValue();

        for (List<Object> row : rows) {
            String rowString = StringUtils.join(row, SEPARATOR);
            Object rowObj;
            try {
                rowObj = this.serde.deserialize(new BytesWritable(rowString.getBytes(Configuration.BDP_ENCODING().getValue())));
            } catch (UnsupportedEncodingException e) {
                throw new SerDeException("deserialize row failed.", e);
            }
            for (int i = 0; i < fieldRefs.size(); ++i) {
                StructField fieldRef = fieldRefs.get(i);
                ObjectInspector fieldOI = fieldRef.getFieldObjectInspector();
                Object fieldData = soi.getStructFieldData(rowObj, fieldRef);
                deserializedFields[i] = SerDeUtils.toThriftPayload(fieldData, fieldOI, protocol);
            }

            rowSet.addRow(deserializedFields);
        }
        return rowSet;
    }

    private void initSerDe() throws SQLException {
        if (this.serde != null) {
            return;
        }
        List<FieldSchema> fieldSchemas = this.mResultSchema.getFieldSchemas();
        StringBuilder namesSb = new StringBuilder();
        StringBuilder typesSb = new StringBuilder();
        if (fieldSchemas != null && !fieldSchemas.isEmpty()) {
            for(int pos = 0; pos < fieldSchemas.size(); ++pos) {
                if (pos != 0) {
                    namesSb.append(",");
                    typesSb.append(",");
                }

                namesSb.append((fieldSchemas.get(pos)).getName());
                typesSb.append((fieldSchemas.get(pos)).getType());
            }
        }

        String names = namesSb.toString();
        String types = typesSb.toString();
        Properties props = new Properties();
        if (names.length() > 0) {
            LOG.debug("Column names: " + names);
            props.setProperty("columns", names);
        }

        if (types.length() > 0) {
            LOG.debug("Column types: " + types);
            props.setProperty("columns.types", types);
        }
        props.setProperty("field.delim", SEPARATOR);
        try {
            this.serde = new LazySimpleSerDe();
            SerDeUtils.initializeSerDe(this.serde, this.queryState.getConf(), props, null);
        } catch (Exception e) {
            throw new SQLException("Could not create Serializator: " + e.getMessage(), e);
        }
    }

    public SQLOperationDisplay getSQLOperationDisplay() {
        return this.sqlOpDisplay;
    }

    @Override
    protected void onNewState(OperationState state, OperationState prevState) {
        super.onNewState(state, prevState);
        this.currentSQLStateScope = this.updateOperationStateMetrics(this.currentSQLStateScope, "hs2_sql_operation_", "hs2_completed_sql_operation_", state);
        Metrics metrics = MetricsFactory.getInstance();
        if (metrics != null) {
            if (state == OperationState.RUNNING && prevState != state) {
                this.incrementUserQueries(metrics);
            }

            if (prevState == OperationState.RUNNING && prevState != state) {
                this.decrementUserQueries(metrics);
            }
        }

        if (state == OperationState.FINISHED || state == OperationState.CANCELED || state == OperationState.ERROR) {
            this.sqlOpDisplay.setRuntime(this.getOperationComplete() - this.getOperationStart());
            if (metrics != null && this.submittedQryScp != null) {
                metrics.endScope(this.submittedQryScp);
            }
        }

        if (state == OperationState.CLOSED) {
            this.sqlOpDisplay.closed();
        } else {
            this.sqlOpDisplay.updateState(state);
        }

        if (state == OperationState.ERROR) {
            this.markQueryMetric(MetricsFactory.getInstance(), "hs2_failed_queries");
        }

        if (state == OperationState.FINISHED) {
            this.markQueryMetric(MetricsFactory.getInstance(), "hs2_succeeded_queries");
        }

    }

    private void incrementUserQueries(Metrics metrics) {
        String username = this.parentSession.getUserName();
        if (username != null) {
            synchronized(userQueries) {
                AtomicInteger count = userQueries.get(username);
                if (count == null) {
                    count = new AtomicInteger(0);
                    AtomicInteger prev = userQueries.put(username, count);
                    if (prev == null) {
                        metrics.incrementCounter("hs2_sql_operation_active_user");
                    } else {
                        count = prev;
                    }
                }

                count.incrementAndGet();
            }
        }

    }

    private void decrementUserQueries(Metrics metrics) {
        String username = this.parentSession.getUserName();
        if (username != null) {
            synchronized(userQueries) {
                AtomicInteger count = userQueries.get(username);
                if (count != null && count.decrementAndGet() <= 0) {
                    metrics.decrementCounter("hs2_sql_operation_active_user");
                    userQueries.remove(username);
                }
            }
        }

    }

    private void markQueryMetric(Metrics metric, String name) {
        if (metric != null) {
            metric.markMeter(name);
        }

    }

    public String getExecutionEngine() {
        return null;
    }

    private final class BackgroundWork implements Runnable {

        @Override
        public void run() {
            SQLOperation.this.registerCurrentOperationLog();
            try {
                SQLOperation.this.setState(OperationState.RUNNING);
                prepareAsync();
                SQLOperation.this.runQuery();
                SQLOperation.this.setState(OperationState.FINISHED);
            } catch (Throwable e) {
                if(e instanceof HiveSQLException) {
                    SQLOperation.this.setOperationException((HiveSQLException) e);
                } else {
                    SQLOperation.this.setOperationException(new HiveSQLException("run query failed.", e));
                }
                try {
                    SQLOperation.this.setState(OperationState.ERROR);
                } catch (HiveSQLException ex) {
                    // ignore it.
                }
                Operation.LOG.error("Error running hive query: ", e);
            } finally {
                SQLOperation.this.unregisterLoggingContext();
                SQLOperation.this.unregisterOperationLog();
            }

        }
    }
}
