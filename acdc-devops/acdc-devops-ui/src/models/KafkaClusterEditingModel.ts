import {useState} from 'react'

export default () => {

	const [kafkaClusterEditingModel, setKafkaClusterEditingModel] = useState<API.KafkaClusterEditingModel>({
		showModal: false,
	})

	return {
		kafkaClusterEditingModel,
		setKafkaClusterEditingModel
	}
}
