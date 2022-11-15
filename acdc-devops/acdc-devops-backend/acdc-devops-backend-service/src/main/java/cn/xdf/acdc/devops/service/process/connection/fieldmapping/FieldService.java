package cn.xdf.acdc.devops.service.process.connection.fieldmapping;

import cn.xdf.acdc.devops.core.domain.dto.DataSetDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;

import java.util.Map;
import java.util.Set;

public interface FieldService {

    /**
     * 支持的 app 类型集合.
     * @return Set
     */
    Set<DataSystemType> supportAppTypes();

    /**
     * 获取数据集字段信息.
     * @param dataSet dataSet
     * @return 表字段信息
     */
    Map<String, FieldDTO> descDataSet(DataSetDTO dataSet);
}
