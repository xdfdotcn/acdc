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

package cn.xdf.acdc.connect.hdfs.storage;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 {@link FilePath}.
 */
public class FilePathTest {

    @Test
    public void testJointPath() {
        String parent = "hdfs://hostname:8020";
        String sub1 = "/user/";
        String sub2 = "hive/";
        String sub3 = "/test";
        String sub4 = "test01";
        String expectPath = "hdfs://hostname:8020/user/hive/test/test01";
        String actualPath = FilePath.of(parent)
            .join(sub1)
            .join(sub2)
            .join(sub3)
            .join(sub4)
            .build().path();
        assertEquals(expectPath, actualPath);

        parent = "user";
        sub1 = "hive/";
        sub2 = "/test";
        sub3 = "/test01/";
        sub4 = "test02";
        expectPath = "/user/hive/test/test01/test02";
        actualPath = FilePath.of(parent)
            .join(sub1)
            .join(sub2)
            .join(sub3)
            .join(sub4)
            .build().path();

        assertEquals(expectPath, actualPath);
    }

    @Test
    public void testExtractVersion() {
        String fileName = "sink_city+0+0000000000+0000000001.orc";
        long offset = HdfsFileOperator.extractVersion(fileName);
        long expectOffset = 1;
        assertEquals(expectOffset, offset);
    }
}
