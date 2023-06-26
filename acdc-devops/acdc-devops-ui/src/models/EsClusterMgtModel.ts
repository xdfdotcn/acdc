import {useState} from 'react'

export default () => {

	const [esClusterMgtModel, setEsClusterMgtModel] = useState<API.EsClusterMgtModel>({
	})

	return {
    esClusterMgtModel,
		setEsClusterMgtModel,
	}
}
