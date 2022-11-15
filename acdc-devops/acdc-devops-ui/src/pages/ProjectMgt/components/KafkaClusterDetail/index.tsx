import React, {useEffect, useRef, useState} from 'react';
import {useModel} from 'umi';
import {Descriptions, Modal} from 'antd';
import ProCard from '@ant-design/pro-card';
import {getKafkaCluster, getRdb} from '@/services/a-cdc/api';
const KafkaClusterDetail: React.FC = () => {

	const {kafkaClusterDetailModel} = useModel('KafkaClusterDetailModel')

	const [kafkaClusterDetail, setKafkaClusterDetail] = useState<API.KafkaCluster>({});

	useEffect(() => {
		initData();
	}, [kafkaClusterDetailModel.kafkaClusterId]);


	const initData = async () => {
		let kafkaCluster: API.KafkaCluster = await getKafkaCluster({kafkaClusterId: kafkaClusterDetailModel.kafkaClusterId})
		setKafkaClusterDetail({
			kafkaCluserType: 'USER',
			name: kafkaCluster!.name,
			version: '2.6.3',
			description: kafkaCluster.description,
			bootstrapServers: kafkaCluster.bootstrapServers,
			securityProtocol: kafkaCluster.securityProtocol,
			saslMechanism: kafkaCluster.saslMechanism,
			saslUsername: kafkaCluster.saslUsername,
			saslPassword: kafkaCluster.saslPassword
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
					<Descriptions.Item label="类型">
						{kafkaClusterDetail.kafkaCluserType}
					</Descriptions.Item>
					<Descriptions.Item label="版本">
						{kafkaClusterDetail.version}
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
					<Descriptions.Item label="sasl.jaas.config.password">
						{kafkaClusterDetail.saslPassword}
					</Descriptions.Item>
				</Descriptions>
			</ProCard>
		</div>
	)
};
export default KafkaClusterDetail;
