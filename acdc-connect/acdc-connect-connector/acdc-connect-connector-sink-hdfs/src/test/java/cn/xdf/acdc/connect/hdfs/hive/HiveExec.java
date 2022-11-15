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

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

@Slf4j
public class HiveExec {

    private static final String HIVE_SASL_ENABLED = "hive.metastore.sasl.enabled";

    private HiveConf hiveConf;

    private CliDriver cliDriver;

    /**
     * HiveExec constructor.
     * @param config HDFS Connector configuration
     */
    public HiveExec(final HdfsSinkConfig config) {
        hiveConf = new HiveConf();
        String hiveConfDir = config.getString(HiveConfig.HIVE_CONF_DIR_CONFIG);
        hiveConf.addResource(new Path(hiveConfDir, "hive-site.xml"));
        String hiveMetaStoreUris = config.getString(HiveConfig.HIVE_METASTORE_URIS_CONFIG);
        hiveConf.set("hive.metastore.uris", hiveMetaStoreUris);
        SessionState.start(new CliSessionState(hiveConf));
        cliDriver = new CliDriver();
    }

    /**
     * Execute a Hive query.
     * @param query The query to execute in Hive
     * @throws Exception query fail
     */
    public void executeQuery(final String query) throws Exception {
        try {
            log.debug("Using in-process Hive instance.");
            String[] argv = getHiveArgs("-S", "-e", query);

            int status = cliDriver.run(argv);
            if (status != 0) {
                throw new IOException("Hive CliDriver exited with status=" + status);
            }
        } catch (IOException e) {
            throw new IOException("Exception thrown in Hive", e);
        }
    }

    private String[] getHiveArgs(final String... args) {
        List<String> newArgs = new LinkedList<>();
        newArgs.addAll(Arrays.asList(args));
        if (hiveConf.getBoolean(HIVE_SASL_ENABLED, false)) {
            newArgs.add("--hiveconf");
            newArgs.add("hive.metastore.sasl.enabled=true");
        }
        return newArgs.toArray(new String[newArgs.size()]);
    }
}
