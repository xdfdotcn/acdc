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

package cn.xdf.acdc.connect.jdbc.util;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CachedConnectionProviderTest {

    private ConnectionProvider provider = mock(ConnectionProvider.class);

    @Test
    public void retryTillFailure() throws SQLException {
        doThrow(new SQLException("test")).when(provider).getConnection();
        int retries = 15;
        ConnectionProvider connectionProvider = new CachedConnectionProvider(provider, retries, 100L);
        try {
            connectionProvider.getConnection();
        } catch (ConnectException connectException) {
            assertNotNull(connectException);
        }
    }

    @Test
    public void retryTillConnect() throws SQLException {
        Connection connection = mock(Connection.class);
        int retries = 15;

        ConnectionProvider connectionProvider = new CachedConnectionProvider(provider, retries, 100L);
        when(provider.getConnection()).thenThrow(new SQLException("test")).thenThrow(new SQLException("test")).thenReturn(connection);
        assertNotNull(connectionProvider.getConnection());
    }
}
