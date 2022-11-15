/** Connector 模块数据状态使用 */

import {useState} from 'react'

export default () => {

	const [projectUserModel, setProjectUserModel] = useState<API.ProjectUserModel>({
	})

	return {
		projectUserModel,
		setProjectUserModel,
	}
}
