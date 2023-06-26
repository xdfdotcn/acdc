package cn.xdf.acdc.devops.service.process.datasystem.es;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.EsDocField;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.EsHelperService;

@RunWith(SpringRunner.class)
public class EsDataSystemMetadataServiceImplTest {

    @Mock
    private EsHelperService helperService;

    @Mock
    private DataSystemResourceService dataSystemResourceService;

    private EsDataSystemMetadataServiceImpl dataSystemMetadataServiceImpl;

    @Before
    public void setUp() throws Exception {
        dataSystemMetadataServiceImpl = new EsDataSystemMetadataServiceImpl();
        ReflectionTestUtils.setField(dataSystemMetadataServiceImpl, "dataSystemResourceService", dataSystemResourceService);
        ReflectionTestUtils.setField(dataSystemMetadataServiceImpl, "helperService", helperService);
    }

    @Test
    public void testCheckDataSystem() {
        DataSystemResourceDetailDTO clusterRsDetail = TestHelper.createClusterRsDetal();

        Mockito.when(dataSystemResourceService.getDetailById(Mockito.any()))
                .thenReturn(clusterRsDetail);

        dataSystemMetadataServiceImpl.checkDataSystem(1L);

        Mockito.verify(helperService)
                .checkCluster(
                        Mockito.eq(TestHelper.NODE_SERVERS),
                        Mockito.eq(TestHelper.U_PASSWORD)
                );
    }

    @Test
    public void testRefreshIndexs() {
        DataSystemResourceDetailDTO clusterRsDetail = TestHelper.createClusterRsDetal();
        List<String> indexs = TestHelper.createIndexNameLimit2();
        Mockito.when(helperService.getClusterAllIndex(
                Mockito.eq(TestHelper.NODE_SERVERS),
                Mockito.eq(TestHelper.U_PASSWORD))
        ).thenReturn(indexs);

        dataSystemMetadataServiceImpl.refreshIndexs(clusterRsDetail);

        @SuppressWarnings("unchecked") ArgumentCaptor<List<DataSystemResourceDetailDTO>> captor = ArgumentCaptor
                .forClass(List.class);
        Mockito.verify(dataSystemResourceService, Mockito.times(1))
                .mergeAllChildrenByName(
                        captor.capture(),
                        Mockito.eq(DataSystemResourceType.ELASTIC_SEARCH_INDEX),
                        Mockito.eq(clusterRsDetail.getId()));

        Map<String, String> indexMap = indexs.stream().collect(Collectors.toMap(it -> it, it -> it));

        captor.getValue().forEach(it -> {
            Assertions.assertThat(indexMap.get(it.getName())).isEqualTo(it.getName());
            Assertions.assertThat(it.getResourceType()).isEqualTo(DataSystemResourceType.ELASTIC_SEARCH_INDEX);
            Assertions.assertThat(it.getParentResource().getId()).isEqualTo(clusterRsDetail.getId());
        });
    }

    @Test
    public void testGetDataSystemTypeShouldReturnEs() {
        DataSystemType type = dataSystemMetadataServiceImpl.getDataSystemType();
        Assertions.assertThat(type).isEqualTo(DataSystemType.ELASTIC_SEARCH);
    }

    @Test
    public void testGetDataCollectionDefinition() {
        long dataCollectionId = 1L;
        DataSystemResourceDetailDTO clusterRsDetail = TestHelper.createClusterRsDetal();
        DataSystemResourceDTO indexResource = TestHelper.createIndexRs();
        List<EsDocField> docFields = TestHelper.createDocFieldLimit2();
        Map<String, EsDocField> docFieldMap = docFields.stream()
                .collect(Collectors.toMap(it -> it.getName(), it -> it));

        Mockito.when(dataSystemResourceService.getById(Mockito.eq(dataCollectionId)))
                .thenReturn(indexResource);

        Mockito.when(dataSystemResourceService.getDetailParent(
                Mockito.eq(dataCollectionId),
                Mockito.eq(DataSystemResourceType.ELASTIC_SEARCH_CLUSTER))
        )
                .thenReturn(clusterRsDetail);

        Mockito.when(helperService.getIndexMapping(
                Mockito.eq(TestHelper.NODE_SERVERS),
                Mockito.eq(TestHelper.U_PASSWORD),
                Mockito.eq(indexResource.getName()))
        )
                .thenReturn(docFields);

        DataCollectionDefinition dataCollectionDefinition = dataSystemMetadataServiceImpl
                .getDataCollectionDefinition(dataCollectionId);

        dataCollectionDefinition.getLowerCaseNameToDataFieldDefinitions().forEach((k, v) -> {
            Assertions.assertThat(docFieldMap.get(k).getName()).isEqualTo(v.getName());
            Assertions.assertThat(docFieldMap.get(k).getType()).isEqualTo(v.getType());
        });
    }

    @Test(expected = ServerErrorException.class)
    public void testGetDataCollectionDefinitionShouldThrowException() {
        long dataCollectionId = 1L;
        DataSystemResourceDetailDTO clusterRsDetail = TestHelper.createClusterRsDetal();
        DataSystemResourceDTO indexResource = TestHelper.createIndexRs();

        Mockito.when(dataSystemResourceService.getById(Mockito.eq(dataCollectionId)))
                .thenReturn(indexResource);

        Mockito.when(dataSystemResourceService.getDetailParent(
                Mockito.eq(dataCollectionId),
                Mockito.eq(DataSystemResourceType.ELASTIC_SEARCH_CLUSTER))
        )
                .thenReturn(clusterRsDetail);

        Mockito.when(helperService.getIndexMapping(
                Mockito.eq(TestHelper.NODE_SERVERS),
                Mockito.eq(TestHelper.U_PASSWORD),
                Mockito.eq(indexResource.getName()))
        ).thenThrow(new ServerErrorException("Error"));

        dataSystemMetadataServiceImpl.getDataCollectionDefinition(dataCollectionId);
    }
}
