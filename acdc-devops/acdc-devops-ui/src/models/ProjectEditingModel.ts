/** Connector 模块数据状态使用 */

import {useState} from 'react'

export default () => {

	const [projectEditingModel, setProjectEditingModel] = useState<API.ProjectEditingModel>({
		showDrawer: false,
	})

	// 已经测试过,...赋值,后面相同的key可以覆盖之前的
	// eg: {...connectorModel,refreshVersion:1}
	return {
		projectEditingModel,
		setProjectEditingModel,
	}
}
