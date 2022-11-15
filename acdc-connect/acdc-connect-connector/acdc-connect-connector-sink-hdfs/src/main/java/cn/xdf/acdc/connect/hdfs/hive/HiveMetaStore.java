/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package cn.xdf.acdc.connect.hdfs.hive;

import cn.xdf.acdc.connect.hdfs.hive.errors.HiveMetaStoreException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.thrift.TException;

@Slf4j
public class HiveMetaStore {

    public static final HiveMetaStore EMPTY_META_STORE = null;

    private final IMetaStoreClient client;

    public HiveMetaStore(final AbstractConfig connectorConfig) {
        this(new Configuration(), connectorConfig);
    }

    public HiveMetaStore(final Configuration conf, final AbstractConfig connectorConfig)
        throws HiveMetaStoreException {
        HiveConf hiveConf = new HiveConf(conf, HiveConf.class);
        String hiveConfDir = connectorConfig.getString(HiveConfig.HIVE_CONF_DIR_CONFIG);
        String hiveMetaStoreUris = connectorConfig.getString(HiveConfig.HIVE_METASTORE_URIS_CONFIG);
        if (hiveMetaStoreUris.isEmpty()) {
            log.warn(
                "hive.metastore.uris empty, an embedded Hive metastore will be created in the directory"
                    + " the connector is started. You need to start Hive in that specific directory to "
                    + "query the data."
            );
        }
        if (!"".equals(hiveConfDir)) {
            String hiveSitePath = hiveConfDir + "/hive-site.xml";
            File hiveSite = new File(hiveSitePath);
            if (!hiveSite.exists()) {
                log.warn(
                    "hive-site.xml does not exist in provided Hive configuration directory {}.",
                    hiveConf
                );
            }
            hiveConf.addResource(new Path(hiveSitePath));
        }
        hiveConf.set("hive.metastore.uris", hiveMetaStoreUris);
        try {
            client = HCatUtil.getHiveMetastoreClient(hiveConf);
        } catch (IOException | MetaException e) {
            throw new HiveMetaStoreException(e);
        }
    }

    protected <R> R doAction(final ClientAction<R> action) throws HiveMetaStoreException {
        // No need to implement retries here. We use RetryingMetaStoreClient which creates a proxy
        // for a IMetaStoreClient implementation and retries calls to it on failure. The retrying
        // client is conscious of the socket timeout and does not call reconnect on an open
        // connection. Since HiveMetaStoreClient's reconnect method does not check the status of the
        // connection, blind retries may cause a huge spike in the number of connections to the Hive
        // MetaStore.
        // TODO,the Hive version is 1.2.x,not support multithreading access.
        synchronized (HiveMetaStore.class) {
            try {
                return action.call();
            } catch (MetaException e) {
                throw new HiveMetaStoreException("Hive MetaStore exception", e);
            } catch (TException e) {
                throw new HiveMetaStoreException("Exception communicating with the Hive MetaStore", e);
            }
        }
    }

    /**
     * Add partition.
     * @param database database
     * @param tableName tableName
     * @param path partition path
     * @throws HiveMetaStoreException exception on invalid partition path
     */
    public void addPartition(final String database, final String tableName, final String path)
        throws HiveMetaStoreException {
        ClientAction<Void> addPartition = new ClientAction<Void>() {
            @Override
            public Void call() throws TException {
                try {
                    // purposely don't check if the partition already exists because
                    // getPartition(db, table, path) will throw an exception to indicate the
                    // partition doesn't exist also. this way, it's only one call.
                    client.appendPartition(database, tableNameConverter(tableName), path);
                } catch (AlreadyExistsException e) {
                    // this is okay
                    System.out.println(e);
                } catch (InvalidObjectException e) {
                    throw new HiveMetaStoreException(
                        "Invalid partition for " + database + "." + tableNameConverter(tableName) + ": " + path, e
                    );
                }
                return null;
            }
        };

        doAction(addPartition);
    }

    /**
     * Drop partition.
     * @param database database
     * @param tableName tableName
     * @param path partition path
     * @throws HiveMetaStoreException exception on invalid partition path
     */
    public void dropPartition(final String database, final String tableName, final String path)
        throws HiveMetaStoreException {
        ClientAction<Void> dropPartition = new ClientAction<Void>() {
            @Override
            public Void call() throws TException {
                try {
                    client.dropPartition(database, tableNameConverter(tableName), path, false);
                } catch (NoSuchObjectException e) {
                    // this is okay
                } catch (InvalidObjectException e) {
                    throw new HiveMetaStoreException(
                        "Invalid partition for " + database + "." + tableNameConverter(tableName) + ": " + path, e
                    );
                }
                return null;
            }
        };

        doAction(dropPartition);
    }

    /**
     * Create database.
     * @param database database
     * @throws HiveMetaStoreException exception on invalid database name
     */
    public void createDatabase(final String database) throws HiveMetaStoreException {
        ClientAction<Void> create = new ClientAction<Void>() {
            @Override
            public Void call() throws TException {
                try {
                    client.createDatabase(
                        new Database(database, "Database created by Kafka Connect", null, null)
                    );
                } catch (AlreadyExistsException e) {
                    log.warn("Hive database already exists: {}", database);
                } catch (InvalidObjectException e) {
                    throw new HiveMetaStoreException("Invalid database: " + database, e);
                }
                return null;
            }
        };

        doAction(create);
    }

    /**
     * Drop database.
     * @param name database name
     * @param deleteData whether to delete data
     * @throws HiveMetaStoreException exception on invalid database name
     */
    public void dropDatabase(
        final String name,
        final boolean deleteData
    ) throws HiveMetaStoreException {
        ClientAction<Void> drop = new ClientAction<Void>() {
            @Override
            public Void call() throws TException {
                try {
                    client.dropDatabase(name, deleteData, true);
                } catch (NoSuchObjectException e) {
                    // this is ok
                }
                return null;
            }
        };

        doAction(drop);
    }

    /**
     * Create table.
     * @param table  table
     * @throws HiveMetaStoreException exception on invalid table
     */
    public void createTable(final Table table) throws HiveMetaStoreException {
        ClientAction<Void> create = new ClientAction<Void>() {
            @Override
            public Void call() throws TException {
                try {
                    client.createTable(table.getTTable());
                } catch (NoSuchObjectException e) {
                    throw new HiveMetaStoreException(
                        "Hive table not found: " + table.getDbName() + "." + table.getTableName()
                    );
                } catch (AlreadyExistsException e) {
                    // this is ok
                    log.warn("Hive table already exists: {}.{}", table.getDbName(), table.getTableName());
                } catch (InvalidObjectException e) {
                    throw new HiveMetaStoreException("Invalid table", e);
                }
                return null;
            }
        };

        createDatabase(table.getDbName());
        doAction(create);
    }

    /**
     * Alert table.
     * @param table  table
     * @param cascade whether or not cascade
     * @throws HiveMetaStoreException exception on invalid table
     */
    public void alterTable(final Table table, final boolean cascade) throws HiveMetaStoreException {
        ClientAction<Void> alter = new ClientAction<Void>() {
            @Override
            public Void call() throws TException {
                try {
                    client.alter_table(table.getDbName(), table.getTableName(), table.getTTable(), cascade);
                } catch (NoSuchObjectException e) {
                    throw new HiveMetaStoreException(
                        "Hive table not found: " + table.getDbName() + "." + table.getTableName()
                    );
                } catch (InvalidObjectException e) {
                    throw new HiveMetaStoreException("Invalid table", e);
                } catch (InvalidOperationException e) {
                    throw new HiveMetaStoreException("Invalid table change", e);
                }
                return null;
            }
        };

        doAction(alter);
    }

    /**
     * Alert table.
     * @param table  table
     * @throws HiveMetaStoreException exception on invalid table
     */
    public void alterTable(final Table table) throws HiveMetaStoreException {
        alterTable(table, false);
    }

    /**
     * Drop table.
     * @param database  database
     * @param tableName tableName
     * @throws HiveMetaStoreException exception on invalid table
     */
    public void dropTable(final String database, final String tableName) {
        ClientAction<Void> drop = new ClientAction<Void>() {
            @Override
            public Void call() throws TException {
                try {
                    client.dropTable(database, tableNameConverter(tableName), false, true);
                } catch (NoSuchObjectException e) {
                    // this is okay
                }
                return null;
            }
        };

        doAction(drop);
    }

    /**
     * Exists table.
     * @param database  database name
     * @param tableName  table name
     * @return boolean
     * @throws HiveMetaStoreException exception on invalid table or database
     */
    public boolean tableExists(
        final String database,
        final String tableName
    ) throws HiveMetaStoreException {
        ClientAction<Boolean> exists = new ClientAction<Boolean>() {
            @Override
            public Boolean call() throws TException {
                try {
                    return client.tableExists(database, tableNameConverter(tableName));
                } catch (UnknownDBException e) {
                    return false;
                }
            }
        };

        return doAction(exists);
    }

    /**
     * Get table.
     * @param database database name
     * @param tableName table name
     * @return table
     * @throws HiveMetaStoreException exception on get table
     */
    public Table getTable(
        final String database,
        final String tableName
    ) throws HiveMetaStoreException {
        ClientAction<Table> getTable = new ClientAction<Table>() {
            @Override
            public Table call() throws TException {
                try {
                    return new Table(client.getTable(database, tableNameConverter(tableName)));
                } catch (NoSuchObjectException e) {
                    throw new HiveMetaStoreException(
                        "Hive table not found: " + database + "." + tableNameConverter(tableName)
                    );
                }
            }
        };

        Table table = doAction(getTable);
        if (table == null) {
            throw new HiveMetaStoreException(
                "Could not find info for table: " + tableNameConverter(tableName)
            );
        }
        return table;
    }

    /**
     * Get table partitions.
     * @param database database name
     * @param tableName table name
     * @param max  max limit count
     * @return table
     * @throws HiveMetaStoreException exception on get partitions
     */
    public List<String> listPartitions(final String database, final String tableName, final short max)
        throws HiveMetaStoreException {
        ClientAction<List<String>> listPartitions = new ClientAction<List<String>>() {
            @Override
            public List<String> call() throws TException {
                try {
                    List<Partition> partitions = client.listPartitions(
                        database,
                        tableNameConverter(tableName),
                        max
                    );
                    List<String> paths = new ArrayList<>();
                    for (Partition partition : partitions) {
                        paths.add(partition.getSd().getLocation());
                    }
                    return paths;
                } catch (NoSuchObjectException e) {
                    return new ArrayList<>();
                }
            }
        };

        return doAction(listPartitions);
    }

    /**
     * Get all tables by database.
     * @param database database name
     * @return all tables
     * @throws HiveMetaStoreException exception on get all tables by database
     */
    public List<String> getAllTables(final String database) throws HiveMetaStoreException {
        ClientAction<List<String>> getAllTables = new ClientAction<List<String>>() {
            @Override
            public List<String> call() throws TException {
                try {
                    return client.getAllTables(database);
                } catch (NoSuchObjectException e) {
                    return new ArrayList<>();
                }
            }
        };

        return doAction(getAllTables);
    }

    /**
     * Get all databases.
     * @return all databases
     * @throws HiveMetaStoreException exception on get all databases
     */
    public List<String> getAllDatabases() throws HiveMetaStoreException {
        ClientAction<List<String>> create = new ClientAction<List<String>>() {
            @Override
            public List<String> call() throws TException {
                try {
                    return client.getAllDatabases();
                } catch (NoSuchObjectException e) {
                    return new ArrayList<>();
                }
            }
        };

        return doAction(create);
    }

    /**
     * Compatible with table name.
     * @param table  table name
     * @return compatible table name
     */
    public String tableNameConverter(final String table) {
        return table == null ? table : table.replaceAll("[.-]", "_");
    }

    public interface ClientAction<R> {

        /**
         *The call back method for remote procedure call.
         *
         * @return Remote call result
         * @throws TException  remote procedure call failure
         */
        R call() throws TException;
    }
}
