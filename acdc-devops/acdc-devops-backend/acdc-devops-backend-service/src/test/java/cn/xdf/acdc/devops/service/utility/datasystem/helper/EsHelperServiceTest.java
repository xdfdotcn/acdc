package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.springframework.test.context.junit4.SpringRunner;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.mapping.IntegerNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.ObjectProperty;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TextProperty;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.cat.ElasticsearchCatClient;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.GetIndexRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.elastic.clients.elasticsearch.security.ElasticsearchSecurityClient;
import co.elastic.clients.elasticsearch.security.GetUserPrivilegesRequest;
import co.elastic.clients.elasticsearch.security.GetUserPrivilegesResponse;
import co.elastic.clients.elasticsearch.security.IndexPrivilege;
import co.elastic.clients.elasticsearch.security.UserIndicesPrivileges;

@RunWith(SpringRunner.class)
public class EsHelperServiceTest {

    private static final String NODE_SERVERS = "server1:9200,server2:9200";

    private ElasticsearchIndicesClient esIndexClient;

    private ElasticsearchCatClient esCatClient;

    private ElasticsearchSecurityClient esSecurityClient;

    private MockedConstruction<ElasticsearchClient> esClientConstruct;

    private AtomicReference<ElasticsearchClient> esClientAr = new AtomicReference<>();

    @After
    public void after() {
        esClientConstruct.close();
    }

    @Before
    public void before() throws SQLException {
        esIndexClient = Mockito.mock(ElasticsearchIndicesClient.class);
        esCatClient = Mockito.mock(ElasticsearchCatClient.class);
        esSecurityClient = Mockito.mock(ElasticsearchSecurityClient.class);

        esClientConstruct = Mockito.mockConstruction(ElasticsearchClient.class, (m, c) -> {
            esClientAr.set(m);
            Mockito.when(m.indices()).thenReturn(esIndexClient);
            Mockito.when(m.cat()).thenReturn(esCatClient);
            Mockito.when(m.security()).thenReturn(esSecurityClient);
        });
    }

    @Test
    public void testGetClusterAllIndexShouldGetEmptyWhenIndexNonexistence(
    ) throws ElasticsearchException, IOException {
        EsHelperService helper = new EsHelperService();
        UsernameAndPassword usernameAndPassword = new UsernameAndPassword("es", "es");

        Mockito.when(esIndexClient.get(Mockito.any(GetIndexRequest.class)))
                .thenReturn(GetIndexResponse.of(b -> b));

        List<String> indexs = helper.getClusterAllIndex(NODE_SERVERS, usernameAndPassword);

        Assertions.assertThat(indexs.size()).isEqualTo(0);

        Mockito.verify(esClientAr.get(), Mockito.times(1)).shutdown();
    }

    @Test
    public void testGetClusterAllIndexShouldIgnoreIndexStartWithDot(
    ) throws ElasticsearchException, IOException {
        final EsHelperService helper = new EsHelperService();

        final UsernameAndPassword usernameAndPassword = new UsernameAndPassword("es", "es");

        Map<String, IndexState> getIndexResult = new HashMap<>();
        getIndexResult.put(".index1", IndexState.of(b -> b));
        getIndexResult.put("index3", IndexState.of(b -> b));
        getIndexResult.put("index2", IndexState.of(b -> b));

        Mockito.when(esIndexClient.get(Mockito.any(GetIndexRequest.class)))
                .thenReturn(GetIndexResponse.of(b -> b.result(getIndexResult)
                ));

        List<String> expectIndexs = getIndexResult.entrySet().stream()
                .map(it -> it.getKey())
                .filter(it -> !it.startsWith(SystemConstant.Symbol.DOT))
                .collect(Collectors.toList());

        List<String> indexs = helper.getClusterAllIndex(NODE_SERVERS, usernameAndPassword);

        Assertions.assertThat(indexs).containsAll(expectIndexs);
        Assertions.assertThat(indexs.size()).isEqualTo(expectIndexs.size());
    }

    @Test(expected = ServerErrorException.class)
    public void testGetClusterAllIndexShouldThrowExceptionWhenBadReq(
    ) throws ElasticsearchException, IOException {
        EsHelperService helper = new EsHelperService();

        UsernameAndPassword usernameAndPassword = new UsernameAndPassword("es", "es");

        Mockito.when(esIndexClient.get(Mockito.any(GetIndexRequest.class)))
                .thenThrow(new IOException());

        helper.getClusterAllIndex(NODE_SERVERS, usernameAndPassword);
    }

    @Test
    public void testGetIndexMapping(
    ) throws ElasticsearchException, IOException {
        final EsHelperService helper = new EsHelperService();

        final UsernameAndPassword usernameAndPassword = new UsernameAndPassword("es", "es");

        Map<String, Property> objProperties = new HashMap<>();
        TextProperty textProperty = TextProperty.of(b -> b);
        IntegerNumberProperty intProperty = IntegerNumberProperty.of(b -> b);

        objProperties.put("obj_f1", Property.of(b -> b.text(textProperty)));
        objProperties.put("obj_f2", Property.of(b -> b.integer(intProperty)));
        ObjectProperty objProperty = ObjectProperty.of(b -> b.properties(objProperties));

        Map<String, Property> properties = new HashMap<>();
        properties.put("f1", Property.of(b -> b.text(textProperty)));
        properties.put("f2", Property.of(b -> b.integer(intProperty)));
        properties.put("f3", Property.of(b -> b.object(objProperty)));

        TypeMapping typeMapping = TypeMapping.of(b -> b.properties(properties));

        IndexState indexState = IndexState.of(b -> b.mappings(typeMapping));

        Map<String, IndexState> getIndexResult = new HashMap<>();

        String indexName = "index1";

        getIndexResult.put(indexName, indexState);

        Mockito.when(esIndexClient.get(Mockito.any(GetIndexRequest.class)))
                .thenReturn(GetIndexResponse.of(b -> b.result(getIndexResult)));

        List<EsDocField> docFields = helper.getIndexMapping(NODE_SERVERS, usernameAndPassword, indexName);

        Assertions.assertThat(docFields.size()).isEqualTo(properties.size());
        for (EsDocField docField : docFields) {
            Assertions.assertThat(properties.containsKey(docField.getName()))
                    .isTrue();
            Assertions.assertThat(docField.getType())
                    .isEqualTo(properties.get(docField.getName())._kind().name());
        }
    }

    @Test
    public void testGetIndexMappingShouldThrowExceptionWhenIndexStateEerror(
    ) throws ElasticsearchException, IOException {
        EsHelperService helper = new EsHelperService();

        UsernameAndPassword usernameAndPassword = new UsernameAndPassword("es", "es");
        String indexName = "index1";

        Map<String, IndexState> getIndexResult = new HashMap<>();

        getIndexResult.put(indexName, null);
        assertGetIndexMappingResult(helper, NODE_SERVERS, usernameAndPassword, getIndexResult, indexName);

        getIndexResult.put(indexName, IndexState.of(b -> b));
        assertGetIndexMappingResult(helper, NODE_SERVERS, usernameAndPassword, getIndexResult, indexName);

        TypeMapping typeMapping = TypeMapping.of(b -> b.properties(new HashMap<>()));
        getIndexResult.put(indexName, IndexState.of(b -> b.mappings(typeMapping)));
        assertGetIndexMappingResult(helper, NODE_SERVERS, usernameAndPassword, getIndexResult, indexName);
    }

    @Test
    public void testCheckClusterHealth() {
        EsHelperService helper = new EsHelperService();

        UsernameAndPassword usernameAndPassword = new UsernameAndPassword("es", "es");

        helper.checkCluster(NODE_SERVERS, usernameAndPassword);
    }

    @Test(expected = ServerErrorException.class)
    public void testCheckClusterHealthShouldThrowExceptionWhenBadReq() throws ElasticsearchException, IOException {
        EsHelperService helper = new EsHelperService();

        UsernameAndPassword usernameAndPassword = new UsernameAndPassword("es", "es");

        Mockito.when(esCatClient.health())
                .thenThrow(new IOException());

        helper.checkCluster(NODE_SERVERS, usernameAndPassword);
    }

    @Test
    public void testCheckIndexPrivilegesShouldNotPass(
    ) throws ElasticsearchException, IOException {
        final EsHelperService helper = new EsHelperService();

        final UsernameAndPassword usernameAndPassword = new UsernameAndPassword("es", "es");
        final String indexName = "index1";
        final String nodeServers = NODE_SERVERS;

        Mockito.when(esSecurityClient.getUserPrivileges(Mockito.any(GetUserPrivilegesRequest.class)))
                .thenReturn(null);

        Assertions.assertThat(Assertions.catchThrowable(() -> {
            helper.checkIndexPrivileges(nodeServers, usernameAndPassword, indexName);
        })).isInstanceOf(ServerErrorException.class);

        assertCheckIndexPrivileges(
                helper,
                nodeServers,
                usernameAndPassword,
                indexName,
                Lists.newArrayList()
        );

        assertCheckIndexPrivileges(helper,
                nodeServers,
                usernameAndPassword,
                indexName,
                Lists.newArrayList(
                        UserIndicesPrivileges.of(bd -> bd.names(Lists.newArrayList())
                                .privileges(Lists.newArrayList())
                                .allowRestrictedIndices(true)
                        )
                )
        );

        assertCheckIndexPrivileges(helper,
                nodeServers,
                usernameAndPassword,
                indexName,
                Lists.newArrayList(
                        UserIndicesPrivileges.of(bd -> bd.names("index2")
                                .privileges(Lists.newArrayList())
                                .allowRestrictedIndices(true)
                        )
                )
        );

        assertCheckIndexPrivileges(helper,
                nodeServers,
                usernameAndPassword,
                indexName,
                Lists.newArrayList(
                        UserIndicesPrivileges.of(bd -> bd.names("index1")
                                .privileges(Lists.newArrayList())
                                .allowRestrictedIndices(true)
                        )
                )
        );
        assertCheckIndexPrivileges(helper,
                nodeServers,
                usernameAndPassword,
                indexName,
                Lists.newArrayList(
                        UserIndicesPrivileges.of(bd -> bd.names("index1")
                                .allowRestrictedIndices(true)
                                .privileges(Lists.newArrayList(IndexPrivilege.CreateIndex))
                        )
                )
        );
    }

    @Test
    public void testCheckIndexPrivilegesShouldPass(
    ) throws ElasticsearchException, IOException {
        final EsHelperService helper = new EsHelperService();

        final UsernameAndPassword usernameAndPassword = new UsernameAndPassword("es", "es");
        final String indexName = "index1";
        final String nodeServers = NODE_SERVERS;

        List<UserIndicesPrivileges> indices = new ArrayList<>();
        UserIndicesPrivileges userIndicesPrivileges = UserIndicesPrivileges.of(bd -> bd.allowRestrictedIndices(true)
                .privileges(Lists.newArrayList(
                        IndexPrivilege.CreateIndex,
                        IndexPrivilege.ViewIndexMetadata,
                        IndexPrivilege.Write,
                        IndexPrivilege.Read)
                )
                .names("index1", "index2")
        );
        indices.add(userIndicesPrivileges);

        Mockito.when(esSecurityClient.getUserPrivileges())
                .thenReturn(GetUserPrivilegesResponse.of(b -> b.indices(indices)
                        .applications(Lists.newArrayList())
                        .cluster(Lists.newArrayList("test"))
                        .global(Lists.newArrayList())
                        .runAs(Lists.newArrayList())
                ));

        helper.checkIndexPrivileges(nodeServers, usernameAndPassword, indexName);
    }

    private void assertGetIndexMappingResult(
            final EsHelperService helper,
            final String nodeServers,
            final UsernameAndPassword usernameAndPassword,
            final Map<String, IndexState> getIndexResult,
            final String indexName
    ) throws ElasticsearchException, IOException {
        Mockito.when(esIndexClient.get(Mockito.any(GetIndexRequest.class)))
                .thenReturn(GetIndexResponse.of(b -> b.result(getIndexResult)));

        Assertions.assertThat(Assertions.catchThrowable(() -> {
            helper.getIndexMapping(nodeServers, usernameAndPassword, indexName);
        })).isInstanceOf(ServerErrorException.class);
    }

    private void assertCheckIndexPrivileges(
            final EsHelperService helper,
            final String nodeServers,
            final UsernameAndPassword usernameAndPassword,
            final String indexName,
            final List<UserIndicesPrivileges> indices
    ) throws ElasticsearchException, IOException {
        Mockito.when(esSecurityClient.getUserPrivileges())
                .thenReturn(GetUserPrivilegesResponse.of(b -> b.indices(indices)
                        .applications(Lists.newArrayList())
                        .cluster(Lists.newArrayList("test"))
                        .global(Lists.newArrayList())
                        .runAs(Lists.newArrayList())
                ));

        Assertions.assertThat(Assertions.catchThrowable(() -> {
            helper.checkIndexPrivileges(nodeServers, usernameAndPassword, indexName);
        })).isInstanceOf(ServerErrorException.class);
    }
}
