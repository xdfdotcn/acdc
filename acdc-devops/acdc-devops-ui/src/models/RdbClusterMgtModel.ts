import {useState} from 'react'

export default () => {

	const [rdbClusterMgtModel, setRdbClusterMgtModel] = useState<API.RdbClusterMgtModel>({
	})

	return {
		rdbClusterMgtModel,
		setRdbClusterMgtModel,
	}
}
