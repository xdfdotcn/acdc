import {useState} from 'react'

export default () => {

	const [starRocksDatasetModel, setStarRocksDatasetModel] = useState<API.StarRocksDatasetModel>({
	})

	return {
		starRocksDatasetModel,
		setStarRocksDatasetModel,
	}
}
