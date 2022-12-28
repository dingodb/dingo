package io.dingodb.index;

import io.dingodb.common.CommonId;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.IndexStatus;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.TableApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.client.connector.impl.ServiceConnector;
import io.dingodb.server.client.meta.service.MetaServiceClient;

import java.util.List;

public class DingoIndexExecutor {

    private CoordinatorConnector coordinatorConnector;
    private String coordinatorSrvAddresses;
    DingoIndexDataExecutor dingoIndexDataExecutor;


    public DingoIndexExecutor(String coordinatorSrvAddresses) {
        this.coordinatorSrvAddresses = coordinatorSrvAddresses;
        this.coordinatorConnector = CoordinatorConnector.getCoordinatorConnector(coordinatorSrvAddresses);
        this.dingoIndexDataExecutor = new DingoIndexDataExecutor(coordinatorConnector);
    }

    public DingoIndexExecutor(CoordinatorConnector coordinatorConnector) {
        this.coordinatorConnector = coordinatorConnector;
        this.dingoIndexDataExecutor = new DingoIndexDataExecutor(coordinatorConnector);
    }

    public void addIndex(String tableName, Index index) throws Exception {
        MetaServiceClient metaServiceClient = new MetaServiceClient(coordinatorConnector);
        TableDefinition tableDefinition = metaServiceClient.getTableDefinition(tableName);

        CommonId tableId = metaServiceClient.getTableId(tableName);

        ServiceConnector serviceConnector = metaServiceClient.getTableConnector(tableId);
        TableApi tableApi = ApiRegistry.getDefault().proxy(TableApi.class, serviceConnector);
        index.setStatus(IndexStatus.BUSY);
        tableApi.createIndex(tableId, index);

        List<Object[]> allFinishedRecord = dingoIndexDataExecutor.getFinishedRecord(tableName);
        for (Object[] record : allFinishedRecord) {
            int retry = 0;
            boolean isRetry = true;
            while (isRetry) {
                try {
                    dingoIndexDataExecutor.executeInsertIndex(tableName, record, index.getName());
                    isRetry = false;
                } catch (Exception e) {
                    retry++;
                    if (retry > 3) {
                        throw e;
                    }
                }
            }
        }

        tableDefinition.removeIndex(index.getName());
        index.setStatus(IndexStatus.NORMAL);
        tableDefinition.addIndex(index);
        tableApi.updateTableDefinition(tableId, tableDefinition);
    }

    public void reboot(String tableName) throws Exception {
        MetaServiceClient metaServiceClient = new MetaServiceClient(coordinatorConnector);
        TableDefinition tableDefinition = metaServiceClient.getTableDefinition(tableName);
        CommonId tableId = metaServiceClient.getTableId(tableName);
        ServiceConnector serviceConnector = metaServiceClient.getTableConnector(tableId);
        TableApi tableApi = ApiRegistry.getDefault().proxy(TableApi.class, serviceConnector);

        List<Object[]> allFinishedRecord = dingoIndexDataExecutor.getFinishedRecord(tableName);
        for (String indexName : tableDefinition.getBusyIndexes()) {
            for (Object[] record : allFinishedRecord) {
                int retry = 0;
                boolean isRetry = true;
                while (isRetry) {
                    try {
                        dingoIndexDataExecutor.executeInsertIndex(tableName, record, indexName);
                        isRetry = false;
                    } catch (Exception e) {
                        retry++;
                        if (retry > 3) {
                            throw e;
                        }
                    }
                }
            }
            Index currentIndex = tableDefinition.getIndex(indexName);
            currentIndex.setStatus(IndexStatus.NORMAL);
            tableDefinition.removeIndex(indexName);
            tableDefinition.addIndex(currentIndex);
            tableApi.updateTableDefinition(tableId, tableDefinition);
        }
    }
}
