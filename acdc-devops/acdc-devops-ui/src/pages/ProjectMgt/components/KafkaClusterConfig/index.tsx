import KafkaClusterDetailModel from '@/models/KafkaClusterDetailModel';
import ProCard from '@ant-design/pro-card';
import { Drawer } from 'antd';
import { useEffect, useState } from 'react';
import {useModel} from 'umi';
import KafkaClusterDetail from '../KafkaClusterDetail';
import KafkaDataset from '../KafkaDataset';
const KafkaClusterConfig: React.FC = () => {
	const [tab, setTab] = useState('tab0');

	const {kafkaClusterConfigModel, setKafkaClusterConfigModel} = useModel('KafkaClusterConfigModel')

	const {kafkaClusterDetailModel, setKafkaClusterDetailModel} = useModel('KafkaClusterDetailModel')

	const {kafkaDatasetModel, setKafkaDatasetModel} = useModel('KafkaDatasetModel')

	useEffect(() => {
		if (kafkaClusterConfigModel.showDrawer) {
			initTabData();
		}
	}, [kafkaClusterConfigModel.kafkaClusterId]);

	const initTabData = () => {
		setKafkaClusterDetailModel({
			...KafkaClusterDetailModel,
			kafkaClusterId: kafkaClusterConfigModel.kafkaClusterId
		})
		setKafkaDatasetModel({
			...kafkaDatasetModel,
			kafkaClusterId: kafkaClusterConfigModel.kafkaClusterId
		})
	}

	return (
		<div>
			<Drawer
				width={"100%"}
				visible={kafkaClusterConfigModel.showDrawer}
				onClose={() => {
					//setShowDetail(false);
					setKafkaClusterConfigModel({
						...kafkaClusterConfigModel,
						showDrawer: false,
					})
				}}
				closable={true}
			>

				<ProCard
					bordered
					hoverable
					tabs={{
						tabPosition: 'top',
						activeKey: tab,
						onChange: (key) => {
							setTab(key);
						},
					}}
					style={{
						minWidth: '100%',
						maxWidth: '100%',
						minHeight: '100%'
					}}
				>
					<ProCard.TabPane key="tab0" tab="集群信息">
						<KafkaClusterDetail />
					</ProCard.TabPane>

					<ProCard.TabPane key="tab1" tab="数据集">
						<KafkaDataset />
					</ProCard.TabPane>
				</ProCard>
			</Drawer>
		</div>

	)
};

export default KafkaClusterConfig;
