package cn.xdf.acdc.devops.service.process.datasystem.widetable;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class WideTableDataSystemResourceDefinitionHolderTest {
    
    @Test
    public void testGetShouldAsExpect() {
        DataSystemResourceDefinition dataSystemResourceDefinition = WideTableDataSystemResourceDefinitionHolder.get();
        Assertions.assertThat(dataSystemResourceDefinition.getType()).isEqualTo(DataSystemResourceType.ACDC_WIDE_TABLE);
    }
}
