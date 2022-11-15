/** Connector 模块数据状态使用 */

import {useState} from 'react'

export default () => {

	const [rdbClusterConfigModel, setRdbClusterConfigModel] = useState<API.RdbClusterConfigModel>({
		showDrawer: false,
	})

	return {
		rdbClusterConfigModel,
		setRdbClusterConfigModel,
	}
}
