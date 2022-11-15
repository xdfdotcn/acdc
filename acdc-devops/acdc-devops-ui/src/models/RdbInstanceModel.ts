/** Connector 模块数据状态使用 */

import {useState} from 'react'

export default () => {

	const [rdbInstanceModel, setRdbInstanceModel] = useState<API.RdbInstanceModel>({
	})
	return {
		rdbInstanceModel,
		setRdbInstanceModel,
	}
}
