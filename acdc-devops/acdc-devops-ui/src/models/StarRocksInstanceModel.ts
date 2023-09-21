/** Connector 模块数据状态使用 */

import {useState} from 'react'

export default () => {

	const [starRocksInstanceModel, setStarRocksInstanceModel] = useState<API.StarRocksInstanceModel>({
	})
	return {
		starRocksInstanceModel,
		setStarRocksInstanceModel,
	}
}
