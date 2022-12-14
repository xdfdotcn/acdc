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

package cn.xdf.acdc.connect.hdfs;

import cn.xdf.acdc.connect.hdfs.common.StorageCommonConfig;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;

public class TestWithSecureMiniDFSCluster extends HdfsSinkConnectorTestBase {

    private static File baseDir;

    private static FileSystem fs;

    private static MiniDFSCluster cluster;

    private static String hdfsPrincipal;

    private static MiniKdc kdc;

    private static String keytab;

    private static String spnegoPrincipal;

    private static String connectorPrincipal;

    private static String connectorKeytab;

    @BeforeClass
    public static void setup() throws Exception {
        initKdc();

        cluster = createDFSCluster();
        fs = cluster.getFileSystem();
    }

    @AfterClass
    public static void cleanup() throws IOException {
        if (fs != null) {
            fs.close();
        }
        if (cluster != null) {
            cluster.shutdown(true);
        }
        UserGroupInformation.reset();
        shutdownKdc();
    }

    private static void initKdc() throws Exception {
        baseDir = new File(System.getProperty("test.build.dir", "target/test-dir"));
        FileUtil.fullyDelete(baseDir);
        assertTrue(baseDir.mkdirs());
        Properties kdcConf = MiniKdc.createConf();
        kdc = new MiniKdc(kdcConf, baseDir);
        kdc.start();

        File keytabFile = new File(baseDir, "hdfs" + ".keytab");
        keytab = keytabFile.getAbsolutePath();
        kdc.createPrincipal(keytabFile, "hdfs" + "/localhost", "HTTP/localhost");
        hdfsPrincipal = "hdfs" + "/localhost@" + kdc.getRealm();
        spnegoPrincipal = "HTTP/localhost@" + kdc.getRealm();

        keytabFile = new File(baseDir, "connect-hdfs" + ".keytab");
        connectorKeytab = keytabFile.getAbsolutePath();
        kdc.createPrincipal(keytabFile, "connect-hdfs/localhost");
        connectorPrincipal = "connect-hdfs/localhost@" + kdc.getRealm();
    }

    private static void shutdownKdc() {
        if (kdc != null) {
            kdc.stop();
        }
        FileUtil.fullyDelete(baseDir);
    }

    /**
     * Set up.
     * @throws Exception exception on set up
     */
    public void setUp() throws Exception {
        Map<String, String> props = createProps();
        connectorConfig = new HdfsSinkConfig(props);
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        if (fs.exists(new Path("/")) && fs.isDirectory(new Path("/"))) {
            for (FileStatus file : fs.listStatus(new Path("/"))) {
                if (file.isDirectory()) {
                    fs.delete(file.getPath(), true);
                } else {
                    fs.delete(file.getPath(), false);
                }
            }
        }
    }

    private static Configuration createSecureConfig(final String dataTransferProtection) throws Exception {
        HdfsConfiguration conf = new HdfsConfiguration();
        SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
        conf.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
        conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, keytab);
        conf.set(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
        conf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, keytab);
        conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
        conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
        conf.set(DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY, dataTransferProtection);
        conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
        conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
        conf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
        conf.setInt(DFSConfigKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);
        //https://issues.apache.org/jira/browse/HDFS-7431
        conf.set(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY,
            "true");
        String keystoresDir = baseDir.getAbsolutePath();
        String sslConfDir = KeyStoreTestUtil.getClasspathDir(TestWithSecureMiniDFSCluster.class);
        KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
        return conf;
    }

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        url = "hdfs://" + cluster.getNameNode().getClientNamenodeAddress();
        props.put(HdfsSinkConfig.HDFS_URL_CONFIG, url);
        props.put(StorageCommonConfig.STORE_URL_CONFIG, url);
        props.put(HdfsSinkConfig.HDFS_AUTHENTICATION_KERBEROS_CONFIG, "true");
        // if we use the connect principal to authenticate with secure Hadoop, the following
        // error shows up: Auth failed for 127.0.0.1:63101:null (GSS initiate failed).
        // As a workaround, we temporarily use namenode principal to authenticate the connector
        // with Hadoop.  The error is probably due to the issue of FQDN in Kerberos.
        // As we have tested the connector on secure multi-node cluster, we will figure out
        // the root cause later.
        props.put(HdfsSinkConfig.CONNECT_HDFS_PRINCIPAL_CONFIG, hdfsPrincipal);
        props.put(HdfsSinkConfig.CONNECT_HDFS_KEYTAB_CONFIG, keytab);
        props.put(HdfsSinkConfig.HDFS_NAMENODE_PRINCIPAL_CONFIG, hdfsPrincipal);
        return props;
    }

    private static MiniDFSCluster createDFSCluster() throws Exception {
        MiniDFSCluster cluster = new MiniDFSCluster
            .Builder(createSecureConfig("authentication"))
            .hosts(new String[] {"localhost", "localhost", "localhost"})
            .nameNodePort(9001)
            .numDataNodes(3)
            .build();
        cluster.waitActive();
        return cluster;
    }
}
