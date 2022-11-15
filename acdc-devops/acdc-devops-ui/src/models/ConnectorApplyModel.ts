import {useState, useCallback} from 'react'


/** connector 申请单数据model*/
export default () => {

	const [applyInfoModel, setApplyInfoModel] = useState<API.ConnectorApplyModel>({})

	const setFieldMappings = (mappings: API.FieldMappingListItem[]) => {
		setApplyInfoModel({...applyInfoModel, fieldMappings: mappings});
	}

	return {
		applyInfoModel,
		setApplyInfoModel,
		setFieldMappings,
	}
}
