import ProCard from '@ant-design/pro-card';
import {ProFormInstance} from '@ant-design/pro-form';
import { Drawer } from 'antd';
import { useRef, useState } from 'react';
import {useModel} from 'umi';
import RdbClusterDetail from '../RdbClusterDetail';
import RdbDataset from '../RdbDataset';
import RdbInstance from '../RdbInstance';

const RdbClusterConfig: React.FC = () => {
	const [tab, setTab] = useState('tab0');
	// 项目详情数据流
	const {rdbClusterConfigModel, setRdbClusterConfigModel} = useModel('RdbClusterConfigModel')
	const {rdbDatasetModel, setRdbDatasetModel} = useModel('RdbDatasetModel')

	const formRef = useRef<
		ProFormInstance<{
			name: string;
			description?: string;
		}>
	>();


	return (
		<div>
			<Drawer
				width={"100%"}
				visible={rdbClusterConfigModel.showDrawer}
				onClose={() => {
					//setShowDetail(false);
					setRdbClusterConfigModel({
						...rdbClusterConfigModel,
						showDrawer: false,
					})
				}}
				closable={true}
			>

			<ProCard
				tabs={{
					tabPosition:'top',
					activeKey: tab,
						onChange: (key) => {
							setTab(key);
							if (key == 'tab2') {
								setRdbDatasetModel({
									...rdbDatasetModel,
									rdbId: rdbClusterConfigModel.rdbId
								})
							}
						},
				}}
					style={{
						minWidth: '100%',
						minHeight: '100%'
					}}
			>
					<ProCard.TabPane key="tab0" tab="集群信息">
						<RdbClusterDetail />
					</ProCard.TabPane>

					<ProCard.TabPane key="tab1" tab="实例管理" >
						<RdbInstance />
					</ProCard.TabPane>
					<ProCard.TabPane key="tab2" tab="数据集">
						<RdbDataset />
					</ProCard.TabPane>
				</ProCard>
			</Drawer>
		</div>

	)
};

export default RdbClusterConfig;
