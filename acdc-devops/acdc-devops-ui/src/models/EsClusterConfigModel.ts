/** Connector 模块数据状态使用 */

import {useState} from 'react'

export default () => {

	const [esClusterConfigModel, setEsClusterConfigModel] = useState<API.EsClusterConfigModel>({
		showDrawer: false,
	})

	return {
		esClusterConfigModel,
    setEsClusterConfigModel,
	}
}
