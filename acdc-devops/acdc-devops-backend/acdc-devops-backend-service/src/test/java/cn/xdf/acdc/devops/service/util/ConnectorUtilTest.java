package cn.xdf.acdc.devops.service.util;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class ConnectorUtilTest {

    @Test
    public void testGetJdbcUrl() {
        String jdbcUrl = ConnectorUtil.getJdbcUrl("192.168.110.1", 3306, "db0", DataSystemType.MYSQL);
        Assertions.assertThat(jdbcUrl).isEqualTo("jdbc:mysql://192.168.110.1:3306/db0");
    }

    @Test
    public void testJoinOnComma() {
        List<String> list = Lists.newArrayList(
            "c",
            "n",
            "f"
        );
        Assertions.assertThat(ConnectorUtil.joinOnComma(list)).isEqualTo("c,n,f");
    }

    @Test
    public void testJoinOn() {
        Assertions.assertThat(ConnectorUtil.joinOn(",", "db0.city", "db0.city2")).isEqualTo("db0.city,db0.city2");
        Assertions.assertThat(ConnectorUtil.joinOn(";", "db0.city:id,tid", "db0.city2:id")).isEqualTo("db0.city:id,tid;db0.city2:id");
    }

    @Test
    public void tesGetRdbSinkConnectorName() {
        String connectorName = ConnectorUtil.getRdbSinkConnectorName(
            DataSystemType.MYSQL,
            "rdb0",
            "db0",
            "tb0"
        );
        Assertions.assertThat(connectorName).isEqualTo("sink-mysql-rdb0-db0-tb0");
        connectorName = ConnectorUtil.getRdbSinkConnectorName(
            DataSystemType.TIDB,
            "rdb0",
            "db0",
            "tb0"
        );

        Assertions.assertThat(connectorName).isEqualTo("sink-tidb-rdb0-db0-tb0");
    }

    @Test
    public void testGetDataTopic() {
        String topic = ConnectorUtil.getDataTopic(DataSystemType.MYSQL, "rdb0", "db0", "tb0");
        Assertions.assertThat(topic).isEqualTo("source-mysql-rdb0-db0-tb0");
    }

    @Test
    public void testGetSchemaHistoryTopic() {
        String topic = ConnectorUtil.getSchemaHistoryTopic(DataSystemType.MYSQL, "rdb0", "db0");
        Assertions.assertThat(topic).isEqualTo("schema_history-source-mysql-rdb0-db0");
    }

    @Test
    public void testGetSourceServerTopic() {
        String topic = ConnectorUtil.getSourceServerTopic(DataSystemType.MYSQL, "rdb0", "db0");
        Assertions.assertThat(topic).isEqualTo("source-mysql-rdb0-db0");
    }

    @Test
    public void testGetSourceServerName() {
        String serverName = ConnectorUtil.getSourceServerName(DataSystemType.MYSQL, "rdb0", "db0");
        Assertions.assertThat(serverName).isEqualTo("source-mysql-rdb0-db0");
    }

    @Test
    public void testGetMessageKeyColumns() {
        List<String> pks = Lists.newArrayList(
            "id",
            "name"
        );
        String conf = ConnectorUtil.getMessageKeyColumns("db0", "tb0", pks);
        Assertions.assertThat(conf).isEqualTo("db0.tb0:id,name");
    }

    @Test
    public void regTest() {
        Pattern pattern = Pattern.compile("(^jdbc:mysql:\\//)(.*)(/)(.*)");
        Matcher m = pattern.matcher("jdbc:mysql://192.168.6.6:3306/debezium_sink?user");
        m.matches();
        String hostAndPort = m.group(2);
        Assertions.assertThat(hostAndPort).isEqualTo("192.168.6.6:3306");

        m = pattern.matcher("jdbc:mysql://www.baidu.com:3306/debezium_sink?user");
        m.matches();
        hostAndPort = m.group(2);
        Assertions.assertThat(hostAndPort).isEqualTo("www.baidu.com:3306");
    }
}
