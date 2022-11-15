/** Connector 模块数据状态使用 */

import {useState} from 'react'

export default () => {

	const [rdbClusterDetailModel, setRdbClusterDetailModel] = useState<API.RdbClusterDetailModel>({
	})

	return {
		rdbClusterDetailModel,
		setRdbClusterDetailModel,
	}
}
