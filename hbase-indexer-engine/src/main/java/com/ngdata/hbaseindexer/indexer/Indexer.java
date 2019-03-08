/*
 * Copyright 2013 NGDATA nv
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
package com.ngdata.hbaseindexer.indexer;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.ngdata.hbaseindexer.ConfigureUtil;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.conf.IndexerConf.RowReadMode;
import com.ngdata.hbaseindexer.metrics.IndexerMetricsUtil;
import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;
import com.ngdata.hbaseindexer.uniquekey.UniqueTableKeyFormatter;
import com.ngdata.sep.util.io.Closer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.ngdata.hbaseindexer.metrics.IndexerMetricsUtil.metricName;

/**
 * The indexing algorithm. It receives an event from the SEP, handles it based on the configuration, and eventually
 * calls Solr.
 */
public abstract class Indexer {

    protected Log log = LogFactory.getLog(getClass());

    private String indexerName;
    protected IndexerConf conf;
    protected final String tableName;
    private Sharder sharder;
    protected UniqueKeyFormatter uniqueKeyFormatter;
    private Timer indexingTimer;

    /**
     * Instantiate an indexer based on the given {@link IndexerConf}.
     */
    public static Indexer createIndexer(String indexerName, IndexerConf conf, String tableName,
                                        Connection tablePool, Sharder sharder) {
        switch (conf.getMappingType()) {
            case COLUMN:
                return new ColumnBasedIndexer(indexerName, conf, tableName, sharder);
            case ROW:
                return new RowBasedIndexer(indexerName, conf, tableName, tablePool, sharder);
            default:
                throw new IllegalStateException("Can't determine the type of indexing to use for mapping type "
                        + conf.getMappingType());
        }
    }

    Indexer(String indexerName, IndexerConf conf, String tableName, Sharder sharder) {
        this.indexerName = indexerName;
        this.conf = conf;
        this.tableName = tableName;
        this.sharder = sharder;
        try {
            this.uniqueKeyFormatter = conf.getUniqueKeyFormatterClass().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Problem instantiating the UniqueKeyFormatter.", e);
        }
        ConfigureUtil.configure(uniqueKeyFormatter, conf.getGlobalParams());
        this.indexingTimer = Metrics.newTimer(metricName(getClass(),
                "Index update calculation timer", indexerName),
                TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    }

    /**
     * Returns the name of this indexer.
     *
     * @return indexer name
     */
    public String getName() {
        return indexerName;
    }


    /**
     * Build all new documents and ids to delete based on a list of {@code RowData}s.
     *
     * @param rowDataList     list of RowData instances to be considered for indexing
     */
    abstract void calculateIndexUpdates(List<RowData> rowDataList) throws IOException;

    /**
     * Create index documents based on a nested list of RowData instances.
     *
     * @param rowDataList list of RowData instances to be considered for indexing
     */
    public void indexRowData(List<RowData> rowDataList) throws IOException, SharderException {
        //
    }

    /**
     * groups a list of ids by shard
     * (consider moving this to a BaseSharder class)
     */
    private Map<Integer, Collection<String>> shardByValue(List<String> idsToDelete) {
        Multimap<Integer, String> map = Multimaps.index(idsToDelete, new Function<String, Integer>() {
            @Override
            public Integer apply(@Nullable String id) {
                try {
                    return sharder.getShard(id);
                } catch (SharderException e) {
                    throw new RuntimeException("error calculating hash", e);
                }
            }
        });
        return map.asMap();
    }

    public void stop() {
        Closer.close(uniqueKeyFormatter);
        IndexerMetricsUtil.shutdownMetrics(indexerName);
    }

    static class RowBasedIndexer extends Indexer {

        private Connection tablePool;
        private Timer rowReadTimer;

        public RowBasedIndexer(String indexerName, IndexerConf conf, String tableName,
                               Connection tablePool,
                               Sharder sharder) {
            super(indexerName, conf, tableName, sharder);
            this.tablePool = tablePool;
            rowReadTimer = Metrics.newTimer(metricName(getClass(), "Row read timer", indexerName), TimeUnit.MILLISECONDS,
                    TimeUnit.SECONDS);
        }

        private Result readRow(RowData rowData) throws IOException {
            TimerContext timerContext = rowReadTimer.time();
            try {
                Table table = tablePool.getTable(TableName.valueOf(rowData.getTable()));
                try {
                    //Get get = mapper.getGet(rowData.getRow());
                    Get get = null;
                    return table.get(get);
                } finally {
                    table.close();
                }
            } finally {
                timerContext.stop();
            }
        }

        @Override
        protected void calculateIndexUpdates(List<RowData> rowDataList) throws IOException {

            Map<String, RowData> idToRowData = calculateUniqueEvents(rowDataList);

            for (RowData rowData : idToRowData.values()) {
                String tableName = new String(rowData.getTable(), Charsets.UTF_8);

                Result result = rowData.toResult();


                boolean rowDeleted = result.isEmpty();
            }
        }

        /**
         * Calculate a map of Solr document ids to relevant RowData, only taking the most recent event for each document id..
         */
        private Map<String, RowData> calculateUniqueEvents(List<RowData> rowDataList) {
            Map<String, RowData> idToEvent = Maps.newHashMap();
            for (RowData rowData : rowDataList) {
                // Check if the event contains changes to relevant key values
                boolean relevant = false;
                for (Cell kv : rowData.getKeyValues()) {
                    //
                }

                if (!relevant) {
                    continue;
                }
                if (uniqueKeyFormatter instanceof UniqueTableKeyFormatter) {
                    idToEvent.put(((UniqueTableKeyFormatter) uniqueKeyFormatter).formatRow(rowData.getRow(),
                            rowData.getTable()), rowData);
                } else {
                    idToEvent.put(uniqueKeyFormatter.formatRow(rowData.getRow()), rowData);
                }

            }
            return idToEvent;
        }

    }

    static class ColumnBasedIndexer extends Indexer {

        public ColumnBasedIndexer(String indexerName, IndexerConf conf, String tableName,
                                  Sharder sharder) {
            super(indexerName, conf, tableName, sharder);
        }

        @Override
        protected void calculateIndexUpdates(List<RowData> rowDataList) throws IOException {
            Map<String, KeyValue> idToKeyValue = calculateUniqueEvents(rowDataList);
            for (Entry<String, KeyValue> idToKvEntry : idToKeyValue.entrySet()) {
                String documentId = idToKvEntry.getKey();

                KeyValue keyValue = idToKvEntry.getValue();
                if (CellUtil.isDelete(keyValue)) {
                    handleDelete(documentId, keyValue, uniqueKeyFormatter);
                } else {
                    Result result = Result.create(Collections.<Cell>singletonList(keyValue));

                }
            }
        }

        private void handleDelete(String documentId, KeyValue deleteKeyValue,
                                  UniqueKeyFormatter uniqueKeyFormatter) {

        }

        /**
         * Delete all values for a single column family from Solr.
         */
        private void deleteFamily(KeyValue deleteKeyValue,
                                  UniqueKeyFormatter uniqueKeyFormatter, byte[] tableName) {
        }

        /**
         * Delete all values for a single row from Solr.
         */
        private void deleteRow(KeyValue deleteKeyValue,
                               UniqueKeyFormatter uniqueKeyFormatter, byte[] tableName) {

        }

        /**
         * Calculate a map of Solr document ids to KeyValue, only taking the most recent event for each document id.
         */
        private Map<String, KeyValue> calculateUniqueEvents(List<RowData> rowDataList) {
            Map<String, KeyValue> idToKeyValue = Maps.newHashMap();
            for (RowData rowData : rowDataList) {
                for (Cell cell : rowData.getKeyValues()) {
                    KeyValue kv = (KeyValue) cell;

                }
            }
            return idToKeyValue;
        }
    }
}
