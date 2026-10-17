/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.backend.store.hbase;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.backend.store.BackendEntry.BackendIterator;
import org.apache.hugegraph.backend.store.BackendSession.AbstractBackendSession;
import org.apache.hugegraph.backend.store.BackendSessionPool;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.exception.NotSupportException;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.apache.hugegraph.util.VersionUtil;
import org.slf4j.Logger;

import com.google.common.util.concurrent.Futures;

public class HbaseSessions extends BackendSessionPool {

    private static final Logger LOG = Log.logger(HbaseSessions.class);

    private static final String COPROCESSOR_AGGR =
            "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
    private static final long SCANNER_CACHING = 1000L;

    private final String namespace;
    private Connection hbase;

    public HbaseSessions(HugeConfig config, String namespace, String store) {
        super(config, namespace + "/" + store);
        this.namespace = namespace;
    }

    protected Connection hbase() {
        E.checkState(this.hbase != null, "HBase connection is not opened");
        return this.hbase;
    }

    private Table table(String table) throws IOException {
        E.checkState(this.hbase != null, "HBase connection is not opened");
        TableName tableName = TableName.valueOf(this.namespace, table);
        return this.hbase.getTable(tableName);
    }

    private AggregationClient aggregationClient() {
        Configuration hConfig = this.hbase.getConfiguration();
        hConfig = HBaseConfiguration.create(hConfig);
        long timeout = this.config().get(HbaseOptions.AGGR_TIMEOUT);
        hConfig.setLong("hbase.rpc.timeout", timeout * 1000L);
        hConfig.setLong("hbase.client.scanner.caching", SCANNER_CACHING);
        return new AggregationClient(hConfig);
    }

    @Override
    public synchronized void open() throws IOException {
        HugeConfig config = this.config();
        String hosts = config.get(HbaseOptions.HBASE_HOSTS);
        int port = config.get(HbaseOptions.HBASE_PORT);
        String znodeParent = config.get(HbaseOptions.HBASE_ZNODE_PARENT);
        boolean isEnableKerberos = config.get(HbaseOptions.HBASE_KERBEROS_ENABLE);
        Configuration hConfig = HBaseConfiguration.create();
        hConfig.set(HConstants.ZOOKEEPER_QUORUM, hosts);
        hConfig.set(HConstants.ZOOKEEPER_CLIENT_PORT, String.valueOf(port));
        hConfig.set(HConstants.ZOOKEEPER_ZNODE_PARENT, znodeParent);

        hConfig.setInt("zookeeper.recovery.retry",
                       config.get(HbaseOptions.HBASE_ZK_RETRY));

        // Set hbase.hconnection.threads.max 64 to avoid OOM(default value: 256)
        hConfig.setInt("hbase.hconnection.threads.max",
                       config.get(HbaseOptions.HBASE_THREADS_MAX));

        String hbaseSite = config.get(HbaseOptions.HBASE_HBASE_SITE);
        hConfig.addResource(new Path(hbaseSite));

        if (isEnableKerberos) {
            String krb5Conf = config.get(HbaseOptions.HBASE_KRB5_CONF);
            System.setProperty("java.security.krb5.conf", krb5Conf);
            String principal = config.get(HbaseOptions.HBASE_KERBEROS_PRINCIPAL);
            String keyTab = config.get(HbaseOptions.HBASE_KERBEROS_KEYTAB);
            hConfig.set("hadoop.security.authentication", "kerberos");
            hConfig.set("hbase.security.authentication", "kerberos");

            //  login/authenticate using keytab
            UserGroupInformation.setConfiguration(hConfig);
            UserGroupInformation.loginUserFromKeytab(principal, keyTab);
        }
        this.hbase = ConnectionFactory.createConnection(hConfig);
    }

    @Override
    protected synchronized boolean opened() {
        // NOTE: isClosed() seems to always return true even if not connected
        return this.hbase != null && !this.hbase.isClosed();
    }

    @Override
    public final Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected Session newSession() {
        return new Session();
    }

    @Override
    protected synchronized void doClose() {
        if (this.hbase == null || this.hbase.isClosed()) {
            return;
        }
        try {
            this.hbase.close();
        } catch (IOException e) {
            throw new BackendException("Failed to close HBase connection", e);
        }
    }

    public boolean existsNamespace() throws IOException {
        try (Admin admin = this.hbase.getAdmin()) {
            for (NamespaceDescriptor ns : admin.listNamespaceDescriptors()) {
                if (this.namespace.equals(ns.getName())) {
                    return true;
                }
            }
        }
        return false;
    }

    public void createNamespace() throws IOException {
        NamespaceDescriptor ns = NamespaceDescriptor.create(this.namespace)
                                                    .build();
        try (Admin admin = this.hbase.getAdmin()) {
            admin.createNamespace(ns);
        }
    }

    public void dropNamespace() throws IOException {
        try (Admin admin = this.hbase.getAdmin()) {
            admin.deleteNamespace(this.namespace);
        }
    }

    /**
     * Whether the table can serve requests: it exists, is enabled and all its
     * regions are assigned (an existing but disabled table is not available).
     */
    public boolean tableAvailable(String table) throws IOException {
        TableName tableName = TableName.valueOf(this.namespace, table);
        try (Admin admin = this.hbase.getAdmin()) {
            return admin.tableExists(tableName) && admin.isTableEnabled(tableName) &&
                   admin.isTableAvailable(tableName);
        }
    }

    public boolean existsTable(String table) throws IOException {
        TableName tableName = TableName.valueOf(this.namespace, table);
        try (Admin admin = this.hbase.getAdmin()) {
            return admin.tableExists(tableName);
        }
    }

    public void createTable(String table, List<byte[]> cfs) throws IOException {
        TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(
                TableName.valueOf(this.namespace, table));
        for (byte[] cf : cfs) {
            tdb.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(cf)
                                                             .build());
        }
        tdb.setCoprocessor(COPROCESSOR_AGGR);
        try (Admin admin = this.hbase.getAdmin()) {
            admin.createTable(tdb.build());
        }
    }

    public void createPreSplitTable(String table, List<byte[]> cfs,
                                    short numOfPartitions) throws IOException {
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(
                TableName.valueOf(this.namespace, table));
        for (byte[] cf : cfs) {
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(cf).build());
        }
        byte[][] splits = new byte[numOfPartitions - 1]
                [org.apache.hadoop.hbase.util.Bytes.SIZEOF_SHORT];
        for (short split = 1; split < numOfPartitions; split++) {
            splits[split - 1] = org.apache.hadoop.hbase.util.Bytes.toBytes(split);
        }
        builder.setCoprocessor(COPROCESSOR_AGGR);
        try (Admin admin = this.hbase.getAdmin()) {
            admin.createTable(builder.build(), splits);
        }
    }

    public void dropTable(String table) throws IOException {
        TableName tableName = TableName.valueOf(this.namespace, table);
        try (Admin admin = this.hbase.getAdmin()) {
            try {
                admin.disableTable(tableName);
            } catch (TableNotEnabledException ignored) {
                // pass
            }
            admin.deleteTable(tableName);
        }
    }

    public void enableTable(String table) throws IOException {
        assert this.existsTable(table);
        TableName tableName = TableName.valueOf(this.namespace, table);
        try (Admin admin = this.hbase.getAdmin()) {
            if (admin.isTableEnabled(tableName)) {
                return;
            }
            try {
                admin.enableTable(tableName);
            } catch (TableNotDisabledException ignored) {
                // pass
            }
        }
    }

    public Future<Void> disableTableAsync(String table) throws IOException {
        assert this.existsTable(table);
        TableName tableName = TableName.valueOf(this.namespace, table);
        try (Admin admin = this.hbase.getAdmin()) {
            try {
                return admin.disableTableAsync(tableName);
            } catch (TableNotEnabledException ignored) {
                // Ignore if it's disabled
                return Futures.immediateFuture(null);
            }
        }
    }

    public Future<Void> truncateTableAsync(String table) throws IOException {
        assert this.existsTable(table);
        TableName tableName = TableName.valueOf(this.namespace, table);
        try (Admin admin = this.hbase.getAdmin()) {
            return admin.truncateTableAsync(tableName, true);
        }
    }

    public void compactTables(List<String> tableNames) throws IOException {
        try (Admin admin = this.hbase.getAdmin()) {
            for (String table : tableNames) {
                admin.compact(TableName.valueOf(this.namespace, table));
            }
        }
    }

    public long storeSize(String table) throws IOException {
        long total = 0L;
        try (Admin admin = this.hbase.getAdmin()) {
            for (ServerName rs : admin.getRegionServers()) {
                // NOTE: we can use getLoad() before hbase 2.0
                //ServerLoad load = admin.getClusterStatus().getLoad(rs);
                //total += load.getStorefileSizeMB() * Bytes.MB;
                //total += load.getMemStoreSizeMB() * Bytes.MB;
                TableName tableName = TableName.valueOf(this.namespace, table);
                for (RegionMetrics m : admin.getRegionMetrics(rs, tableName)) {
                    Double storeFileSize = m.getStoreFileSize().get(Size.Unit.BYTE);
                    total += storeFileSize.longValue();

                    Double memStoreFileSize = m.getMemStoreSize().get(Size.Unit.BYTE);
                    total += memStoreFileSize.longValue();
                }
            }
        }
        return total;
    }

    /**
     * Session interface for HBase
     */
    public interface HbaseSession<R> {

        /**
         * Add a row record to a table
         */
        void put(String table, byte[] family, byte[] rowkey,
                 Collection<BackendColumn> columns);

        /**
         * Add a row record to a table(can be used when adding an index)
         */
        void put(String table, byte[] family, byte[] rowkey, byte[] qualifier,
                 byte[] value);

        /**
         * Delete a record by rowkey and qualifier from a table
         */
        default void remove(String table, byte[] family, byte[] rowkey,
                            byte[] qualifier) {
            this.remove(table, family, rowkey, qualifier, false);
        }

        /**
         * Delete a record by rowkey and qualifier from a table,
         * just delete the latest version of the specified column if need
         */
        void remove(String table, byte[] family, byte[] rowkey,
                    byte[] qualifier, boolean latestVersion);

        /**
         * Delete a record by rowkey from a table
         */
        void delete(String table, byte[] family, byte[] rowkey);

        /**
         * Get a record by rowkey and qualifier from a table
         */
        R get(String table, byte[] family, byte[] rowkey, byte[] qualifier);

        /**
         * Get a record by rowkey from a table
         */
        R get(String table, byte[] family, byte[] rowkey);

        /**
         * Get multi records by rowkeys from a table
         */
        R get(String table, byte[] family, Set<byte[]> rowkeys);

        /**
         * Scan all records from a table
         */
        default R scan(String table, long limit) {
            Scan scan = new Scan();
            if (limit >= 0) {
                scan.setFilter(new PageFilter(limit));
            }
            return this.scan(table, scan);
        }

        /**
         * Scan records by rowkey prefix from a table
         */
        default R scan(String table, byte[] prefix) {
            return this.scan(table, prefix, true, prefix);
        }

        /**
         * Scan records by multi rowkey prefixes from a table
         */
        default R scan(String table, Set<byte[]> prefixes) {
            FilterList orFilters = new FilterList(Operator.MUST_PASS_ONE);
            for (byte[] prefix : prefixes) {
                FilterList andFilters = new FilterList(Operator.MUST_PASS_ALL);
                List<RowRange> ranges = new ArrayList<>();
                ranges.add(new RowRange(prefix, true, null, true));
                andFilters.addFilter(new MultiRowRangeFilter(ranges));
                andFilters.addFilter(new PrefixFilter(prefix));

                orFilters.addFilter(andFilters);
            }

            Scan scan = new Scan().setFilter(orFilters);
            return this.scan(table, scan);
        }

        /**
         * Scan records by rowkey start and prefix from a table
         * TODO: setRowPrefixFilter deprecated since HBase 2.5.0, will be removed in 4.0.0,
         * use setStartStopRowForPrefixScan(byte[]) instead.
         */
        default R scan(String table, byte[] startRow, boolean inclusiveStart,
                       byte[] prefix) {
            final Scan scan = new Scan();
            if (startRow == prefix) {
                scan.setRowPrefixFilter(prefix);
            } else {
                scan.withStartRow(startRow, inclusiveStart)
                    .setFilter(new PrefixFilter(prefix));
            }
            return this.scan(table, scan);
        }

        /**
         * Scan records by rowkey range from a table
         */
        default R scan(String table, byte[] startRow, byte[] stopRow) {
            return this.scan(table, startRow, true, stopRow, false);
        }

        /**
         * Scan records by rowkey range from a table
         */
        default R scan(String table, byte[] startRow, boolean inclusiveStart,
                       byte[] stopRow, boolean inclusiveStop) {
            Scan scan = new Scan().withStartRow(startRow, inclusiveStart);
            if (stopRow != null) {
                scan.withStopRow(stopRow, inclusiveStop);
            }
            return this.scan(table, scan);
        }

        /**
         * Inner scan: send scan request to HBase and get iterator
         */
        R scan(String table, Scan scan);

        /**
         * Increase a counter by rowkey and qualifier to a table
         */
        long increase(String table, byte[] family, byte[] rowkey,
                      byte[] qualifier, long value);
    }

    /**
     * Session implement for HBase
     */
    public class Session extends AbstractBackendSession
            implements HbaseSession<RowIterator> {

        private final Map<String, List<Row>> batch;

        public Session() {
            this.batch = new HashMap<>();
        }

        private void batch(String table, Row row) {
            List<Row> rows = this.batch.get(table);
            if (rows == null) {
                rows = new ArrayList<>();
                this.batch.put(table, rows);
            }
            rows.add(row);
        }

        private int batchSize() {
            int size = 0;
            for (List<Row> puts : this.batch.values()) {
                size += puts.size();
            }
            return size;
        }

        private void checkBatchResults(Object[] results, List<Row> rows)
                throws Throwable {
            assert rows.size() == results.length;
            for (int i = 0; i < results.length; i++) {
                Object result = results[i];
                if (result instanceof Throwable) {
                    throw (Throwable) result;
                }
                if (result == null || !((Result) result).isEmpty()) {
                    throw new BackendException("Failed batch for row: %s",
                                               rows.get(i));
                }
            }
        }

        public Connection hbase() {
            return HbaseSessions.this.hbase();
        }

        public String namespace() {
            return HbaseSessions.this.namespace;
        }

        @Override
        public void open() {
            this.opened = true;
        }

        @Override
        public void close() {
            assert this.closeable();
            this.opened = false;
        }

        @Override
        public boolean closed() {
            return !this.opened || !HbaseSessions.this.opened();
        }

        /**
         * Any change in the session
         */
        @Override
        public boolean hasChanges() {
            return !this.batch.isEmpty();
        }

        /**
         * Commit all updates(put/delete) to DB
         */
        @Override
        public Integer commit() {
            int count = this.batchSize();
            if (count <= 0) {
                return 0;
            }

            // TODO: this will not be atomic, to be improved
            for (Entry<String, List<Row>> action : this.batch.entrySet()) {
                List<Row> rows = action.getValue();
                Object[] results = new Object[rows.size()];
                try (Table table = table(action.getKey())) {
                    table.batch(rows, results);
                    checkBatchResults(results, rows);
                } catch (InterruptedIOException e) {
                    throw new BackendException("Interrupted, " +
                                               "maybe it is timed out", e);
                } catch (Throwable e) {
                    // TODO: Mark and delete committed records
                    throw new BackendException("Failed to commit, " +
                                               "there may be inconsistent states for HBase", e);
                }
            }

            // Clear batch if write() successfully (retained if failed)
            this.batch.clear();

            return count;
        }

        /**
         * Rollback all updates(put/delete) not committed
         */
        @Override
        public void rollback() {
            this.batch.clear();
        }

        /**
         * Add a row record to a table
         */
        @Override
        public void put(String table, byte[] family, byte[] rowkey,
                        Collection<BackendColumn> columns) {
            Put put = new Put(rowkey);
            for (BackendColumn column : columns) {
                put.addColumn(family, column.name, column.value);
            }
            this.batch(table, put);
        }

        /**
         * Add a row record to a table with ttl
         */
        public void put(String table, byte[] family, byte[] rowkey,
                        Collection<BackendColumn> columns, long ttl) {
            Put put = new Put(rowkey);
            for (BackendColumn column : columns) {
                put.addColumn(family, column.name, column.value);
            }
            put.setTTL(ttl);
            this.batch(table, put);
        }

        /**
         * Add a row record to a table(can be used when adding an index)
         */
        @Override
        public void put(String table, byte[] family,
                        byte[] rowkey, byte[] qualifier, byte[] value) {
            Put put = new Put(rowkey);
            put.addColumn(family, qualifier, value);
            this.batch(table, put);
        }

        /**
         * Add a row record to a table with ttl for index
         */
        public void put(String table, byte[] family, byte[] rowkey,
                        byte[] qualifier, byte[] value, long ttl) {
            Put put = new Put(rowkey);
            put.addColumn(family, qualifier, value);
            put.setTTL(ttl);
            this.batch(table, put);
        }

        /**
         * Delete a record by rowkey and qualifier from a table
         */
        @Override
        public void remove(String table, byte[] family,
                           byte[] rowkey, byte[] qualifier) {
            this.remove(table, family, rowkey, qualifier, false);
        }

        /**
         * Delete a record by rowkey and qualifier from a table,
         * just delete the latest version of the specified column if need
         */
        @Override
        public void remove(String table, byte[] family, byte[] rowkey,
                           byte[] qualifier, boolean latestVersion) {
            assert family != null;
            assert rowkey != null;
            E.checkArgument(qualifier != null,
                            "HBase qualifier can't be null when removing");
            Delete delete = new Delete(rowkey);
            if (latestVersion) {
                // Just delete the latest version of the specified column
                delete.addColumn(family, qualifier);
            } else {
                // Delete all versions of the specified column
                delete.addColumns(family, qualifier);
            }
            this.batch(table, delete);
        }

        /**
         * Delete a record by rowkey from a table
         */
        @Override
        public void delete(String table, byte[] family, byte[] rowkey) {
            assert rowkey != null;
            Delete delete = new Delete(rowkey);
            if (family != null) {
                delete.addFamily(family);
            }
            this.batch(table, delete);
        }

        /**
         * Get a record by rowkey and qualifier from a table
         */
        @Override
        public RowIterator get(String table, byte[] family,
                               byte[] rowkey, byte[] qualifier) {
            assert !this.hasChanges();

            Get get = new Get(rowkey);
            get.addColumn(family, qualifier);

            try (Table htable = table(table)) {
                return new RowIterator(htable.get(get));
            } catch (IOException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Get a record by rowkey from a table
         */
        @Override
        public RowIterator get(String table, byte[] family, byte[] rowkey) {
            assert !this.hasChanges();

            Get get = new Get(rowkey);
            if (family != null) {
                get.addFamily(family);
            }

            try (Table htable = table(table)) {
                return new RowIterator(htable.get(get));
            } catch (IOException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Get multi records by rowkeys from a table
         */
        @Override
        public RowIterator get(String table, byte[] family,
                               Set<byte[]> rowkeys) {
            assert !this.hasChanges();

            List<Get> gets = new ArrayList<>(rowkeys.size());
            for (byte[] rowkey : rowkeys) {
                Get get = new Get(rowkey);
                if (family != null) {
                    get.addFamily(family);
                }
                gets.add(get);
            }

            try (Table htable = table(table)) {
                return new RowIterator(htable.get(gets));
            } catch (IOException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Scan records by rowkey range from a table
         */
        @Override
        public RowIterator scan(String table,
                                byte[] startRow, boolean inclusiveStart,
                                byte[] stopRow, boolean inclusiveStop) {
            assert !this.hasChanges();

            Scan scan = new Scan();
            if (startRow != null) {
                // Refer: https://issues.apache.org/jira/browse/HBASE-16498 (Bug Fix)
                scan.withStartRow(startRow, inclusiveStart);
            }
            if (stopRow != null) {
                String version = VersionInfo.getVersion();
                if (inclusiveStop && !VersionUtil.gte(version, "2.0")) {
                    // The parameter stoprow-inclusive doesn't work before v2.0
                    // https://issues.apache.org/jira/browse/HBASE-20675
                    inclusiveStop = false;
                    // Add a trailing 0 byte to stopRow
                    stopRow = Arrays.copyOf(stopRow, stopRow.length + 1);
                }
                if (Bytes.equals(startRow, stopRow) &&
                    inclusiveStart && !inclusiveStop) {
                    // Bug https://issues.apache.org/jira/browse/HBASE-21618
                    return new RowIterator();
                }
                scan.withStopRow(stopRow, inclusiveStop);
            }
            return this.scan(table, scan);
        }

        /**
         * Inner scan: send scan request to HBase and get iterator
         */
        @Override
        public RowIterator scan(String table, Scan scan) {
            assert !this.hasChanges();

            try (Table htable = table(table)) {
                return new RowIterator(htable.getScanner(scan));
            } catch (IOException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Increase a counter by rowkey and qualifier to a table
         */
        @Override
        public long increase(String table, byte[] family, byte[] rowkey,
                             byte[] qualifier, long value) {
            try (Table htable = table(table)) {
                return htable.incrementColumnValue(rowkey, family,
                                                   qualifier, value);
            } catch (IOException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Get store size of specified table
         */
        public long storeSize(String table) throws IOException {
            return HbaseSessions.this.storeSize(table);
        }

        /**
         * Just for debug
         */
        @SuppressWarnings("unused")
        private void dump(String table, Scan scan) throws IOException {
            LOG.info(String.format(">>>> scan table {} with {}", table, scan));
            RowIterator iterator = this.scan(table, scan);
            while (iterator.hasNext()) {
                Result row = iterator.next();
                LOG.info(StringEncoding.format(row.getRow()));
                CellScanner cellScanner = row.cellScanner();
                while (cellScanner.advance()) {
                    Cell cell = cellScanner.current();
                    byte[] key = CellUtil.cloneQualifier(cell);
                    byte[] val = CellUtil.cloneValue(cell);
                    LOG.info("  {}={}",
                             StringEncoding.format(key),
                             StringEncoding.format(val));
                }
            }
        }

        public CountSession countSession() {
            return new CountSession(this);
        }
    }

    public class CountSession implements HbaseSession<Number>, AutoCloseable {

        private final Session origin;
        private final AggregationClient aggrClient;

        public CountSession(Session origin) {
            this.origin = origin;
            this.aggrClient = aggregationClient();
        }

        @Override
        public void put(String table, byte[] family, byte[] rowkey,
                        Collection<BackendColumn> columns) {
            throw new NotSupportException("AggrSession.put");
        }

        @Override
        public void put(String table, byte[] family, byte[] rowkey,
                        byte[] qualifier, byte[] value) {
            throw new NotSupportException("AggrSession.put");
        }

        @Override
        public void remove(String table, byte[] family, byte[] rowkey,
                           byte[] qualifier, boolean latestVersion) {
            throw new NotSupportException("AggrSession.remove");
        }

        @Override
        public void delete(String table, byte[] family, byte[] rowkey) {
            throw new NotSupportException("AggrSession.delete");
        }

        @Override
        public Number get(String table, byte[] family, byte[] rowkey,
                          byte[] qualifier) {
            return count(this.origin.get(table, family, rowkey, qualifier));
        }

        @Override
        public Number get(String table, byte[] family, byte[] rowkey) {
            return count(this.origin.get(table, family, rowkey));
        }

        @Override
        public Number get(String table, byte[] family, Set<byte[]> rowkeys) {
            return count(this.origin.get(table, family, rowkeys));
        }

        private long count(RowIterator iter) {
            long count = 0L;
            while (iter.hasNext()) {
                if (!iter.next().isEmpty()) {
                    count++;
                }
            }
            return count;
        }

        @Override
        public Number scan(String table, Scan scan) {
            LongColumnInterpreter ci = new LongColumnInterpreter();
            try {
                return this.aggrClient.rowCount(table(table), ci, scan);
            } catch (Throwable e) {
                throw new BackendException(e);
            }
        }

        @Override
        public long increase(String table, byte[] family, byte[] rowkey,
                             byte[] qualifier, long value) {
            throw new NotSupportException("AggrSession.increase");
        }

        @Override
        public void close() throws IOException {
            this.aggrClient.close();
        }
    }

    protected static class RowIterator implements BackendIterator<Result> {

        private final ResultScanner resultScanner;
        private final Iterator<Result> results;

        private byte[] position = null;

        public RowIterator(ResultScanner resultScanner) {
            this.resultScanner = resultScanner;
            this.results = resultScanner.iterator();
        }

        public RowIterator(Result... results) {
            this.resultScanner = null;
            List<Result> rs = new ArrayList<>(results.length);
            for (Result result : results) {
                // Get by Ids may return empty result
                if (!result.isEmpty()) {
                    rs.add(result);
                }
            }
            this.results = rs.iterator();
        }

        @Override
        public boolean hasNext() {
            boolean has = this.results.hasNext();
            if (!has) {
                this.position = null;
                this.close();
            }
            return has;
        }

        @Override
        public Result next() {
            // Reset position due to results.next() may throw ex
            this.position = null;

            Result next = this.results.next();

            // Update position for paging
            this.position = next.getRow();

            return next;
        }

        @Override
        public void close() {
            if (this.resultScanner != null) {
                this.resultScanner.close();
            }
        }

        @Override
        public byte[] position() {
            return this.position;
        }
    }
}
