import {useState} from 'react'

export default () => {

	const [starRocksClusterMgtModel, setStarRocksClusterMgtModel] = useState<API.StarRocksClusterMgtModel>({
	})

	return {
		starRocksClusterMgtModel,
		setStarRocksClusterMgtModel,
	}
}
