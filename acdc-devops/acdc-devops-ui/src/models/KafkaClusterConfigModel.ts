/** Connector 模块数据状态使用 */

import {useState} from 'react'

export default () => {

	const [kafkaClusterConfigModel, setKafkaClusterConfigModel] = useState<API.KafkaClusterConfigModel>({
		showDrawer: false,
	})

	return {
		kafkaClusterConfigModel,
		setKafkaClusterConfigModel,
	}
}
