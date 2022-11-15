import {useState} from 'react'

export default () => {

	const [rdbClusterEditingModel, setRdbClusterEditingModel] = useState<API.RdbClusterEditingModel>({
		showDrawer: false,
	})

	return {
		rdbClusterEditingModel,
		setRdbClusterEditingModel,
	}
}
