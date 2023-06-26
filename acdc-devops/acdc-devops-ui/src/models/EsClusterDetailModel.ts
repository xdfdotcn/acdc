/** Connector 模块数据状态使用 */

import {useState} from 'react'

export default () => {

	const [esClusterDetailModel, setEsClusterDetailModel] = useState<API.EsClusterDetailModel>({
	})

	return {
    esClusterDetailModel,
		setEsClusterDetailModel,
	}
}
