import ProCard from '@ant-design/pro-card';
import { Drawer } from 'antd';
import { useEffect, useState } from 'react';
import {useModel} from 'umi';
import EsClusterDetail from '../EsClusterDetail';
import EsDataset from '../EsDataset';
const EsClusterConfig: React.FC = () => {
	const [tab, setTab] = useState('tab0');

	const {esClusterConfigModel, setEsClusterConfigModel} = useModel('EsClusterConfigModel')

	const {esClusterDetailModel, setEsClusterDetailModel} = useModel('EsClusterDetailModel')

	const {esDatasetModel, setEsDatasetModel} = useModel('EsDatasetModel')

	useEffect(() => {
		if (esClusterConfigModel.showDrawer) {
			initTabData();
		}
	}, [esClusterConfigModel.esClusterId]);

	const initTabData = () => {
		setEsClusterDetailModel({
			...esClusterDetailModel,
			esClusterId: esClusterConfigModel.esClusterId
		})
		setEsDatasetModel({
			...esDatasetModel,
			esClusterId: esClusterConfigModel.esClusterId
		})
	}

	return (
		<div>
			<Drawer
				width={"100%"}
				visible={esClusterConfigModel.showDrawer}
				onClose={() => {
					//setShowDetail(false);
					setEsClusterConfigModel({
						...esClusterConfigModel,
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
						<EsClusterDetail />
					</ProCard.TabPane>

					<ProCard.TabPane key="tab1" tab="数据集">
						<EsDataset />
					</ProCard.TabPane>
				</ProCard>
			</Drawer>
		</div>

	)
};

export default EsClusterConfig;
