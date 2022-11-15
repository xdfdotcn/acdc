import {useState} from 'react'

export default () => {

	const [rdbDatasetModel, setRdbDatasetModel] = useState<API.RdbDatasetModel>({
	})

	return {
		rdbDatasetModel,
		setRdbDatasetModel,
	}
}
