/** Connector 模块数据状态使用 */

import {useState} from 'react'

export default () => {

	const [projectDetailModel, setProjectDetailModel] = useState<API.ProjectDetailModel>({
	})

	return {
		projectDetailModel,
		setProjectDetailModel,
	}
}
