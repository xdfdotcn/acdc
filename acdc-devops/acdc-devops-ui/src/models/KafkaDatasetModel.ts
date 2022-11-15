import {useState} from 'react'

export default () => {

	const [kafkaDatasetModel, setKafkaDatasetModel] = useState<API.KafkaDatasetModel>({
	})

	return {
		kafkaDatasetModel,
		setKafkaDatasetModel
	}
}
