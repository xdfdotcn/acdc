// RdbTidb

import {useState} from 'react'

export default () => {

	const [rdbTidbModel, setRdbTidbModel] = useState<API.RdbTidbModel>({refreshVersion: 1})

	// 已经测试过,...赋值,后面相同的key可以覆盖之前的
	// eg: {...connectorModel,refreshVersion:1}
	return {
		rdbTidbModel,
		setRdbTidbModel,
	}

}
