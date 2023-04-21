package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionColumnConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.MetadataSourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class ConnectionRepositoryTest {

    @Autowired
    private ConnectionRepository connectionRepository;

    @Autowired
    private ProjectRepository projectRepository;

    @Autowired
    private DataSystemResourceRepository dataSystemResourceRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private ConnectionColumnConfigurationRepository connectionColumnConfigurationRepository;

    @Autowired
    private EntityManager entityManager;

    @Test
    public void testQueryShouldPass() {
        ConnectionQuery connectionQuery = new ConnectionQuery();
        connectionQuery.setSourceConnectorId(1L);
        connectionQuery.setSinkDataSystemType(DataSystemType.MYSQL);
        connectionQuery.setSinkDataCollectionName("table_1");
        connectionQuery.setDomainAccount("test@acdc.io");
        connectionQuery.setSinkConnectorId(1L);

        connectionRepository.query(connectionQuery);
    }

    @Test
    public void testCascadeQueryShouldGetSameVersionConfiguration() {
        // fake related data
        UserDO user = saveUser();
        ProjectDO project = saveProject(user);
        DataSystemResourceDO dataSystemResource = saveDataSystemResourceDO();

        ConnectionDO connection = new ConnectionDO();
        connection.setDesiredState(ConnectionState.STOPPED);
        connection.setActualState(ConnectionState.STOPPED);

        connection.setSourceProject(project);
        connection.setSourceDataCollection(dataSystemResource);
        connection.setSourceDataSystemType(DataSystemType.MYSQL);

        connection.setSinkDataCollection(dataSystemResource);
        connection.setSinkDataSystemType(DataSystemType.MYSQL);
        connection.setSinkProject(project);
        connection.setUser(user);

        connection.setVersion(2);

        connection.setRequisitionState(RequisitionState.APPROVING);
        // save connection
        connectionRepository.save(connection);

        // save two version configuration
        saveConfiguration(1, connection);
        saveConfiguration(2, connection);

        entityManager.flush();
        entityManager.clear();

        // assertion
        ConnectionDO resultConnection = connectionRepository.getOne(connection.getId());
        resultConnection.getConnectionColumnConfigurations().forEach(each -> {
            Assertions.assertThat(each.getConnectionVersion()).isEqualTo(connection.getVersion());
        });
    }

    private UserDO saveUser() {
        UserDO user = new UserDO()
                .setEmail("acdc@acdc.io")
                .setName("acdc")
                .setCreatedBy("acdc");
        return userRepository.save(user);
    }

    private ProjectDO saveProject(final UserDO owner) {
        ProjectDO project = new ProjectDO()
                .setOwner(owner)
                .setName("acdc_test_project")
                .setSource(MetadataSourceType.USER_INPUT);
        return projectRepository.save(project);
    }

    private DataSystemResourceDO saveDataSystemResourceDO() {
        DataSystemResourceDO dataSystemResource = new DataSystemResourceDO();
        dataSystemResource.setDataSystemType(DataSystemType.MYSQL);
        dataSystemResource.setResourceType(DataSystemResourceType.MYSQL_TABLE);
        dataSystemResource.setName("test_table");
        return dataSystemResourceRepository.save(dataSystemResource);
    }

    private ConnectionColumnConfigurationDO saveConfiguration(final Integer version, final ConnectionDO connection) {
        ConnectionColumnConfigurationDO connectionColumnConfiguration = new ConnectionColumnConfigurationDO();
        connectionColumnConfiguration.setConnection(connection);
        connectionColumnConfiguration.setConnectionVersion(version);
        connectionColumnConfiguration.setFilterOperator("");
        connectionColumnConfiguration.setFilterValue("");
        connectionColumnConfiguration.setSinkColumnName("sink_column_name_version_" + version);
        connectionColumnConfiguration.setSinkColumnType("");
        connectionColumnConfiguration.setSinkColumnUniqueIndexNames("");

        connectionColumnConfiguration.setSourceColumnName("source_column_name_version_" + version);
        connectionColumnConfiguration.setSourceColumnType("");
        connectionColumnConfiguration.setSourceColumnUniqueIndexNames("");
        return connectionColumnConfigurationRepository.save(connectionColumnConfiguration);
    }
}
