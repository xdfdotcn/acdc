/** Connector 模块数据状态使用 */

import {useState} from 'react'

export default () => {

	const [starRocksClusterDetailModel, setStarRocksClusterDetailModel] = useState<API.StarRocksClusterDetailModel>({
	})

	return {
		starRocksClusterDetailModel,
		setStarRocksClusterDetailModel,
	}
}
