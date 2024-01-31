package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.constant.SystemConstant.Symbol;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ExpandWildcard;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch.indices.GetIndexRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.elastic.clients.elasticsearch.security.GetUserPrivilegesResponse;
import co.elastic.clients.elasticsearch.security.IndexPrivilege;
import co.elastic.clients.elasticsearch.security.UserIndicesPrivileges;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.extern.slf4j.Slf4j;

/**
 * Es helper service.
 */
@Service
@Slf4j
public class EsHelperService {
    
    public static final String HTTPS_PREFIX = "https://";
    
    public static final String HTTP_PREFIX = "http://";
    
    private static final Set<IndexPrivilege> REQUIRED_INDEX_PRIVILEGES = Sets.newHashSet(
            IndexPrivilege.CreateIndex,
            IndexPrivilege.Read,
            IndexPrivilege.Write,
            IndexPrivilege.ViewIndexMetadata
    );
    
    /**
     * Get cluster's all indexs.
     *
     * @param nodeServers node servers
     * @param usernameAndPassword username and password
     * @return index list
     */
    public List<String> getClusterAllIndex(
            final String nodeServers,
            final UsernameAndPassword usernameAndPassword
    ) {
        return getClusterAllIndex(nodeServers, usernameAndPassword, name -> !name.startsWith(Symbol.DOT));
    }
    
    private List<String> getClusterAllIndex(
            final String nodeServers,
            final UsernameAndPassword usernameAndPassword,
            final Predicate<String> nameFilter
    ) {
        return execute(nodeServers, usernameAndPassword, client -> {
            // 只获取对外开放的索引
            GetIndexRequest request = GetIndexRequest.of(bd -> bd
                    .index(Symbol.MULTIPLY)
                    .expandWildcards(Lists.newArrayList(ExpandWildcard.Open))
                    .flatSettings(true)
            );
            
            GetIndexResponse response = doGetIndexReq(request, client);
            
            Map<String, IndexState> result = response.result();
            
            if (CollectionUtils.isEmpty(result)) {
                return Collections.emptyList();
            }
            
            return result.entrySet().stream()
                    .map(it -> it.getKey())
                    .filter(nameFilter)
                    .collect(Collectors.toList());
        });
    }
    
    /**
     * Get the index's mapping.
     *
     * @param nodeServers node servers
     * @param usernameAndPassword username and password
     * @param indexName index name
     * @return field list
     */
    public List<EsDocField> getIndexMapping(
            final String nodeServers,
            final UsernameAndPassword usernameAndPassword,
            final String indexName
    ) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(indexName), "index name must not be empty");
        
        return execute(nodeServers, usernameAndPassword, client -> {
            GetIndexRequest request = GetIndexRequest.of(bd -> bd.index(indexName).flatSettings(true));
            
            GetIndexResponse response = doGetIndexReq(request, client);
            
            Map<String, IndexState> result = response.result();
            
            List<EsDocField> docFields = new ArrayList<>();
            
            String errorMessage = String.format("Index mapping does not exist, %s", indexName);
            
            if (CollectionUtils.isEmpty(result) || Objects.isNull(result.get(indexName))) {
                throw new ServerErrorException(errorMessage);
            }
            
            IndexState indexState = result.get(indexName);
            if (Objects.isNull(indexState)
                    || Objects.isNull(indexState.mappings())
                    || CollectionUtils.isEmpty(indexState.mappings().properties())
            ) {
                throw new ServerErrorException(errorMessage);
            }
            
            Map<String, Property> properties = indexState.mappings().properties();
            for (Entry<String, Property> entry : properties.entrySet()) {
                // TODO Object 后续可能需要特殊处理
                String name = entry.getKey();
                String type = entry.getValue()._kind().name();
                
                docFields.add(new EsDocField(name, type));
            }
            
            return docFields;
        });
    }
    
    private GetIndexResponse doGetIndexReq(
            final GetIndexRequest request,
            final ElasticsearchClient client) {
        try {
            return client.indices().get(request);
        } catch (ElasticsearchException | IOException e) {
            log.error("An error occurred while sending the [get index]", e);
            
            String errorMessage = String.format("An error occurred get cluster index, "
                    + "please check the configured address and authentication or index name.");
            
            throw new ServerErrorException(errorMessage);
        }
    }
    
    /**
     * Check cluster health.
     *
     * @param nodeServers node servers
     * @param usernameAndPassword username and password
     */
    public void checkCluster(
            final String nodeServers,
            final UsernameAndPassword usernameAndPassword
    ) {
        
        execute(nodeServers, usernameAndPassword, client -> {
            doCatHealthReq(client);
            return null;
        });
    }
    
    private void doCatHealthReq(
            final ElasticsearchClient client
    ) {
        try {
            client.cat().health();
        } catch (ElasticsearchException | IOException e) {
            String errorMessage = String.format("An error occurred checking the cluster health, "
                    + "please check the configured address and authentication.");
            
            log.error("An error occurred while sending the [cat health]", e);
            
            throw new ServerErrorException(errorMessage);
        }
    }
    
    /**
     * Check index privileges.
     *
     * @param nodeServers node servers
     * @param usernameAndPassword username and password
     * @param indexName index name
     */
    
    public void checkIndexPrivileges(
            final String nodeServers,
            final UsernameAndPassword usernameAndPassword,
            final String indexName
    ) {
        execute(nodeServers, usernameAndPassword, client -> {
            doCheckIndexPrivilegesReq(client, indexName);
            return null;
        });
    }
    
    private void doCheckIndexPrivilegesReq(
            final ElasticsearchClient client,
            final String indexName
    ) {
        String errorMessage = String.format("An error occurred getting index privileges,"
                + " please check the configured address and authentication."
                + " required index privileges: %s", REQUIRED_INDEX_PRIVILEGES
        );
        
        try {
            GetUserPrivilegesResponse respone = client.security().getUserPrivileges();
            if (Objects.isNull(respone) || CollectionUtils.isEmpty(respone.indices())) {
                throw new ServerErrorException(errorMessage);
            }
            
            List<UserIndicesPrivileges> userIndicesPrivilegesList = respone.indices();
            for (UserIndicesPrivileges userIndicesPrivileges : userIndicesPrivilegesList) {
                
                // 1. index must be privileged
                List<String> privilegedIndexNames = userIndicesPrivileges.names();
                if (CollectionUtils.isEmpty(privilegedIndexNames)
                        || !privilegedIndexNames.contains(indexName)
                ) {
                    throw new ServerErrorException(errorMessage);
                }
                
                // 2. check index required privileges
                List<IndexPrivilege> indexPrivileges = userIndicesPrivileges.privileges();
                if (CollectionUtils.isEmpty(indexPrivileges)
                        || !indexPrivileges.containsAll(REQUIRED_INDEX_PRIVILEGES)
                ) {
                    throw new ServerErrorException(errorMessage);
                }
            }
        } catch (ElasticsearchException | IOException e) {
            log.error("An error occurred while sending the [get user privileges]", e);
            throw new ServerErrorException(errorMessage);
        }
    }
    
    private <R> R execute(
            final String nodeServers,
            final UsernameAndPassword usernameAndPassword,
            final Function<ElasticsearchClient, R> callback
    ) {
        // init client
        BasicCredentialsProvider credsProv = new BasicCredentialsProvider();
        credsProv.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(
                usernameAndPassword.getUsername(),
                usernameAndPassword.getPassword()));
        
        List<String> nodeServerList = Splitter.on(SystemConstant.Symbol.COMMA)
                .splitToList(nodeServers.replaceAll("\\s", SystemConstant.EMPTY_STRING));
        
        HttpHost[] httpHosts = new HttpHost[nodeServerList.size()];
        for (int i = 0; i < nodeServerList.size(); i++) {
            httpHosts[i] = getHttpHost(nodeServerList.get(i));
        }
        
        RestClient restClient = RestClient
                .builder(httpHosts)
                .setHttpClientConfigCallback(hc -> hc.setDefaultCredentialsProvider(credsProv)
                        .addInterceptorLast((HttpResponseInterceptor) (response, context) -> response
                                .addHeader("X-Elastic-Product", "Elasticsearch"))).build();
        
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        
        ElasticsearchClient client = new ElasticsearchClient(transport);
        
        // handler request
        try {
            R result = callback.apply(client);
            return result;
        } finally {
            try {
                transport.close();
                restClient.close();
                client.shutdown();
            } catch (IOException e) {
                String errorMessage = String.format("An error occurred close resources, error message: %s",
                        e.getMessage());
                
                throw new ServerErrorException(errorMessage);
            }
        }
    }
    
    protected HttpHost getHttpHost(final String nodeServer) {
        String prefixRemovedNodeServer = nodeServer.startsWith(HTTPS_PREFIX) ? nodeServer.substring(HTTPS_PREFIX.length())
                : nodeServer.startsWith(HTTP_PREFIX) ? nodeServer.substring(HTTP_PREFIX.length())
                : nodeServer;
        
        List<String> hostAndPort = Splitter.on(Symbol.COLON).splitToList(prefixRemovedNodeServer);
        String host = hostAndPort.get(0);
        String port = hostAndPort.get(1);
        return new HttpHost(host, Integer.parseInt(port));
    }
}

