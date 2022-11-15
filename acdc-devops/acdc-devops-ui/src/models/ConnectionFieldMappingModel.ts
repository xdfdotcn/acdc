/** 字段映射公共组件 */

import {useState} from 'react'

export default () => {

	const [connectionFieldMappingModel, setConnectionFieldMappingModel] = useState<API.ConnectionFieldMappingModel>({})

	const setConnectionFieldMappings = (mappings: API.FieldMappingListItem[]) => {
		setConnectionFieldMappingModel({...connectionFieldMappingModel, fieldMappings: mappings});
	}


	// 已经测试过,...赋值,后面相同的key可以覆盖之前的
	// eg: {...connectorModel,refreshVersion:1}
	return {
		connectionFieldMappingModel,
		setConnectionFieldMappingModel,
		setConnectionFieldMappings,
	}
}
