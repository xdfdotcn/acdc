// rdbMysql
import {useState} from 'react'

export default () => {

	const [rdbMysqlModel, setRdbMysqlModel] = useState<API.RdbMysqlModel>({})

	// 已经测试过,...赋值,后面相同的key可以覆盖之前的
	// eg: {...connectorModel,refreshVersion:1}
	return {
		rdbMysqlModel,
		setRdbMysqlModel
	}
}
