import {useState} from 'react'

export default () => {

	const [esDatasetModel, setEsDatasetModel] = useState<API.EsDatasetModel>({
	})

	return {
    esDatasetModel,
		setEsDatasetModel
	}
}
