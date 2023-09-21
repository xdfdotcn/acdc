/** Connector 模块数据状态使用 */

import {useState} from 'react'

export default () => {

	const [starRocksClusterConfigModel, setStarRocksClusterConfigModel] = useState<API.StarRocksClusterConfigModel>({
		showDrawer: false,
	})

	return {
    starRocksClusterConfigModel,
    setStarRocksClusterConfigModel,
	}
}
