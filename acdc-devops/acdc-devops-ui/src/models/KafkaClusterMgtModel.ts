import {useState} from 'react'

export default () => {

	const [kafkaClusterMgtModel, setKafkaClusterMgtModel] = useState<API.KafkaClusterMgtModel>({
	})

	return {
		kafkaClusterMgtModel,
		setKafkaClusterMgtModel,
	}
}
