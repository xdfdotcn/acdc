import ProCard from '@ant-design/pro-card';
import {ProFormInstance} from '@ant-design/pro-form';
import {Drawer} from 'antd';
import { useRef, useState } from 'react';
import {useModel} from 'umi';
import KafkaClusterMgt from '../KafkaClusterMgt';
import ProjectDetail from '../ProjectDetail';
import ProjectUser from '../ProjectUser';
import RdbClusterMgt from '../RdbClusterMgt';
const ProjectConfig: React.FC = () => {
	const [tab, setTab] = useState('tab0');
	// 项目详情数据流
	const {projectConfigModel,setProjectConfigModel} = useModel('ProjectConfigModel')

	const formRef = useRef<
		ProFormInstance<{
			name: string;
			description?: string;
		}>
	>();


	return (
			<Drawer
				width={"100%"}
				visible={projectConfigModel.showDrawer}
				onClose={() => {
					//setShowDetail(false);
					setProjectConfigModel({
						...projectConfigModel,
						showDrawer: false,
					})
				}}
				closable={true}
			>
			<ProCard
				tabs={{
					tabPosition: 'top',
					activeKey: tab,
					onChange: (key) => {
						setTab(key);
					},
				}}
				style={{
					minWidth: '100%',
					minHeight: '100%'
				}}
			>
				<ProCard.TabPane key="tab0" tab="项目信息" >
					<ProjectDetail />
				</ProCard.TabPane>

				<ProCard.TabPane key="tab1" tab="用户管理" >
					<ProjectUser />
				</ProCard.TabPane>

				<ProCard.TabPane key="tab2" tab="RDB">
					<RdbClusterMgt />
				</ProCard.TabPane>

				<ProCard.TabPane key="tab3" tab="KAFKA">
					<KafkaClusterMgt />
				</ProCard.TabPane>

				<ProCard.TabPane key="tab4" tab="HIVE">
					HIVE
				</ProCard.TabPane>

			</ProCard>
			</Drawer>
	)
};

export default ProjectConfig;
