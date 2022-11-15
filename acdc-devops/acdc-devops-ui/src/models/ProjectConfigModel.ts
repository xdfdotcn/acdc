/** Connector 模块数据状态使用 */

import {useState} from 'react'

export default () => {

	const [projectConfigModel, setProjectConfigModel] = useState<API.ProjectConfigModel>({
		showDrawer: false,
	})

	return {
		projectConfigModel,
		setProjectConfigModel,
	}
}
