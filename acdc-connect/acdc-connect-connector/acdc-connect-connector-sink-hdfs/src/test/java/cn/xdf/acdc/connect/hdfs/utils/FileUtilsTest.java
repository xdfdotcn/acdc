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

package cn.xdf.acdc.connect.hdfs.utils;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 {@link FileUtils}.
 */
public class FileUtilsTest {

    @Test
    public void jointPath() {
        String parent = "hdfs://hostname:8020";
        String sub1 = "/user/";
        String sub2 = "hive/";
        String sub3 = "/test";
        String sub4 = "test01";
        String expectPath = "hdfs://hostname:8020/user/hive/test/test01";
        String actualPath = FileUtils.jointPath(parent, sub1, sub2, sub3, sub4);
        assertEquals(expectPath, actualPath);

        parent = "user";
        sub1 = "hive/";
        sub2 = "/test";
        sub3 = "/test01/";
        sub4 = "test02";
        expectPath = "/user/hive/test/test01/test02";
        actualPath = FileUtils.jointPath(parent, sub1, sub2, sub3, sub4);
        assertEquals(expectPath, actualPath);
    }

    @Test
    public void createPathWitchUrl() {
        String storeUrl = "hdfs://hostname:8020";
        String path = "user/test";
        String expectPath = "hdfs://hostname:8020/user/test";
        String actualPath = FileUtils.createPathWitchUrl(storeUrl, path);
        assertEquals(expectPath, actualPath);

        path = "/user/test/";
        actualPath = FileUtils.createPathWitchUrl(storeUrl, path);
        assertEquals(expectPath, actualPath);

        path = "/user/test";
        actualPath = FileUtils.createPathWitchUrl(storeUrl, path);
        assertEquals(expectPath, actualPath);

        path = "user/test/";
        actualPath = FileUtils.createPathWitchUrl(storeUrl, path);
        assertEquals(expectPath, actualPath);
    }

    @Test
    public void repairPath() {
        String path = "user/test1/";
        String expectPath = "user/test1";
        String actualPath = FileUtils.repairPath(path);
        assertEquals(expectPath, actualPath);

        path = "/user/test1/";
        actualPath = FileUtils.repairPath(path);
        assertEquals(expectPath, actualPath);

        path = "user/test1";
        actualPath = FileUtils.repairPath(path);
        assertEquals(expectPath, actualPath);
    }
}
