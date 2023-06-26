import {useState} from 'react'

export default () => {

	const [esClusterEditingModel, setEsClusterEditingModel] = useState<API.EsClusterEditingModel>({
		showModal: false,
	})

	return {
    esClusterEditingModel,
		setEsClusterEditingModel
	}
}
