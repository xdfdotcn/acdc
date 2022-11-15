/** Connector 模块数据状态使用 */

import {useState} from 'react'

export default () => {

	const [kafkaClusterDetailModel, setKafkaClusterDetailModel] = useState<API.KafkaClusterDetailModel>({
	})

	return {
		kafkaClusterDetailModel,
		setKafkaClusterDetailModel,
	}
}
