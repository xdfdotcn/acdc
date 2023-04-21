import React, {useEffect, useRef, useState} from 'react';
import {useModel} from 'umi';
import {Descriptions} from 'antd';
import ProCard from '@ant-design/pro-card';
import {getDataSystemResource} from '@/services/a-cdc/api';
const KafkaClusterDetail: React.FC = () => {

	const {kafkaClusterDetailModel} = useModel('KafkaClusterDetailModel')

	const [kafkaClusterDetail, setKafkaClusterDetail] = useState<API.KafkaCluster>({});

	useEffect(() => {
		initData();
	}, [kafkaClusterDetailModel.kafkaClusterId]);

	const initData = async () => {
    // fixme: why kafkaClusterDetailModel.kafkaClusterId can by undefined
    if (!kafkaClusterDetailModel.kafkaClusterId) {
      return
    }

		let kafkaCluster: API.DataSystemResource = await getDataSystemResource({id: kafkaClusterDetailModel.kafkaClusterId})
		setKafkaClusterDetail({
      id: kafkaCluster.id,
      name: kafkaCluster.name,
      bootstrapServers: kafkaCluster.dataSystemResourceConfigurations["bootstrap.servers"].value,
      description: kafkaCluster.description,
      securityProtocol: kafkaCluster.dataSystemResourceConfigurations["security.protocol"].value,
      saslMechanism: kafkaCluster.dataSystemResourceConfigurations["sasl.mechanism"] ? kafkaCluster.dataSystemResourceConfigurations["sasl.mechanism"].value : "",
      saslUsername: kafkaCluster.dataSystemResourceConfigurations["username"] ? kafkaCluster.dataSystemResourceConfigurations["username"].value : ""
		})
	}

	return (
		<div>
			<ProCard
				title="集群信息"
				hoverable
				bordered
				headerBordered
				onCollapse={(collapse) => {}}
				style={{
					maxWidth: '100%',
					minHeight: '100%'
				}}
			>
				<Descriptions column={1}>
					<Descriptions.Item label="集群名称">
						{kafkaClusterDetail.name}
					</Descriptions.Item>
					<Descriptions.Item label="集群地址">
						{kafkaClusterDetail.bootstrapServers}
					</Descriptions.Item>
					<Descriptions.Item label="security.protocol">
						{kafkaClusterDetail.securityProtocol}
					</Descriptions.Item>
					<Descriptions.Item label="sasl.mechanism">
						{kafkaClusterDetail.saslMechanism}
					</Descriptions.Item>
					<Descriptions.Item label="sasl.jaas.config.username">
						{kafkaClusterDetail.saslUsername}
					</Descriptions.Item>
				</Descriptions>
			</ProCard>
		</div>
	)
};
export default KafkaClusterDetail;
