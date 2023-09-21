package cn.xdf.acdc.devops.api.biz.v1;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemServiceManager;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@RestController
@RequestMapping("api/v1/biz")
public class BizDataSystemResourceController {
    @Autowired
    private DataSystemResourceService dataSystemResourceService;
    
    @Autowired
    private DataSystemServiceManager dataSystemServiceManager;
    
    /**
     * Validate data collection.
     *
     * @param dataCollectionIds data collection id list
     * @return true or false
     */
    @PostMapping("/data-system-resources/data-collection/validate")
    public Boolean validateDataCollection(@RequestBody final List<Long> dataCollectionIds) {
        List<DataCollectionDefinition> dataCollectionDefinitions = new ArrayList<>(dataCollectionIds.size());
        for (Long dataCollectionId : dataCollectionIds) {
            DataSystemType dataSystemType = dataSystemResourceService.getDataSystemType(dataCollectionId);
            DataCollectionDefinition dataCollectionDefinition = dataSystemServiceManager.getDataSystemMetadataService(dataSystemType)
                    .getDataCollectionDefinition(dataCollectionId);
            dataCollectionDefinitions.add(dataCollectionDefinition);
        }
        
        Map<String, String> nameAndTypeMapping = new HashMap<>();
        Map<String, DataFieldDefinition> anyOneLowerCaseNameToDataFieldDefinitions = dataCollectionDefinitions.get(0).getLowerCaseNameToDataFieldDefinitions();
        for (Entry<String, DataFieldDefinition> entry : anyOneLowerCaseNameToDataFieldDefinitions.entrySet()) {
            nameAndTypeMapping.put(entry.getKey(), entry.getValue().getType());
        }
        
        for (DataCollectionDefinition definition : dataCollectionDefinitions) {
            Map<String, DataFieldDefinition> lowerCaseNameToDataFieldDefinitions = definition.getLowerCaseNameToDataFieldDefinitions();
            if (lowerCaseNameToDataFieldDefinitions.size() != nameAndTypeMapping.size()) {
                return false;
            }
            for (Entry<String, DataFieldDefinition> entry : lowerCaseNameToDataFieldDefinitions.entrySet()) {
                String fieldName = entry.getKey();
                DataFieldDefinition fieldDefinition = entry.getValue();
                if (!nameAndTypeMapping.containsKey(fieldName)
                        || !fieldDefinition.getType().equals(nameAndTypeMapping.get(fieldName))
                ) {
                    return false;
                }
            }
        }
        return true;
    }
}
