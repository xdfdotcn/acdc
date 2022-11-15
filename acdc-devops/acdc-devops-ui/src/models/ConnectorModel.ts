/** Connector 模块数据状态使用 */

import {useState} from 'react'

export default () => {

	const [connectorModel, setConnectorModel] = useState<API.ConnectorModel>({
		refreshVersion: 1,
		showEdit: false,
		showDetail: false,
	})


	const sinkHiveLink = (): API.SinkConnectorInfo => {
		return connectorModel.sinkConnectorInfo!;
	}

	const sinkRdbLink = (): API.SinkConnectorInfo => {
		return connectorModel.sinkConnectorInfo!;
	}

	const sourceRdbLink = (): API.SourceConnectorInfo => {
		return connectorModel.sourceConnectorInfo!;
	}

	const config = (): Map<string, string> => {
		return connectorModel.connectorConfig!;
	}

	// 已经测试过,...赋值,后面相同的key可以覆盖之前的
	// eg: {...connectorModel,refreshVersion:1}
	return {
		connectorModel,
		setConnectorModel,
		sourceRdbLink,
		sinkRdbLink,
		sinkHiveLink,
		config,
	}
}
