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

import cn.xdf.acdc.connect.hdfs.partitioner.DefaultPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Map;

public class HiveTestUtils {

    /**
     * Get partitioner.
     * @param parsedConfig config
     * @return partitioner
     */
    public static Partitioner getPartitioner(final Map<String, Object> parsedConfig) {
        Partitioner partitioner = new DefaultPartitioner();
        partitioner.configure(parsedConfig);
        return partitioner;
    }

    /**
     * Parse out result.
     * @param output  out result
     * @return parse result array
     * */
    public static String[] parseOutput(final String output) {
        return output.replace(" ", "").split("\t");
    }

    /**
     * Execute HQL.
     * @param hiveExec hiveExec
     * @param query query HQL
     * @return query result
     * @throws Exception exception on execute HQL
     */
    public static String runHive(final HiveExec hiveExec, final String query) throws Exception {
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        PrintStream outSaved = System.out;
        PrintStream errSaved = System.err;
        System.setOut(new PrintStream(outBytes, true));
        System.setErr(new PrintStream(errBytes, true));
        try {
            hiveExec.executeQuery(query);
        } finally {
            System.setOut(outSaved);
            System.setErr(errSaved);
        }
        ByteArrayInputStream outBytesIn = new ByteArrayInputStream(outBytes.toByteArray());
        ByteArrayInputStream errBytesIn = new ByteArrayInputStream(errBytes.toByteArray());
        BufferedReader is = new BufferedReader(new InputStreamReader(outBytesIn));
        BufferedReader es = new BufferedReader(new InputStreamReader(errBytesIn));
        StringBuilder output = new StringBuilder();
        String line;
        while ((line = is.readLine()) != null) {
            if (output.length() > 0) {
                output.append("\n");
            }
            output.append(line);
        }
        if (output.length() == 0) {
            output = new StringBuilder();
            while ((line = es.readLine()) != null) {
                output.append("\n");
                output.append(line);
            }
        }
        return output.toString();
    }
}
