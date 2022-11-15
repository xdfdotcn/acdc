/** 字段映射公共组件 */

import {useState} from 'react'

export default () => {

	const [fieldMappingModel, setFieldMappingModel] = useState<API.FieldMappingModel>({})

	const setFieldMappings = (mappings: API.FieldMappingListItem[]) => {
		setFieldMappingModel({...fieldMappingModel, fieldMappings: mappings});
	}


	// 已经测试过,...赋值,后面相同的key可以覆盖之前的
	// eg: {...connectorModel,refreshVersion:1}
	return {
		fieldMappingModel,
		setFieldMappingModel,
		setFieldMappings,
	}
}
