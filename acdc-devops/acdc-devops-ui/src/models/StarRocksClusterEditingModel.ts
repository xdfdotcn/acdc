import {useState} from 'react'

export default () => {

	const [starRocksClusterEditingModel, setStarRocksClusterEditingModel] = useState<API.StarRocksClusterEditingModel>({
		showDrawer: false,
	})

	return {
		starRocksClusterEditingModel,
		setStarRocksClusterEditingModel,
	}
}
