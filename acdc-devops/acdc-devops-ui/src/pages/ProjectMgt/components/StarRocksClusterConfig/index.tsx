import ProCard from '@ant-design/pro-card';
import { Drawer } from 'antd';
import { useState } from 'react';
import {useModel} from 'umi';
import StarRocksClusterDetail from '../StarRocksClusterDetail';
import StarRocksDataCollection from '../StarRocksDataCollection';
import StarRocksInstance from '../StarRocksInstance';

const StarRocksClusterConfig: React.FC = () => {
	const [tab, setTab] = useState('tab0');
	// 项目详情数据流
	const {starRocksClusterConfigModel, setStarRocksClusterConfigModel} = useModel('StarRocksClusterConfigModel')
	const {starRocksDatasetModel, setStarRocksDatasetModel} = useModel('StarRocksDatasetModel')

	return (
		<div>
			<Drawer
				width={"100%"}
				visible={starRocksClusterConfigModel.showDrawer}
				onClose={() => {
					//setShowDetail(false);
					setStarRocksClusterConfigModel({
						...starRocksClusterConfigModel,
						showDrawer: false,
					})
          setTab("tab0")
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
              setStarRocksDatasetModel({
                ...starRocksDatasetModel,
                clusterResourceId: starRocksClusterConfigModel.resourceId,
                dataSystemType: starRocksClusterConfigModel.dataSystemType
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
						<StarRocksClusterDetail />
					</ProCard.TabPane>
					<ProCard.TabPane key="tab1" tab="实例管理" >
						<StarRocksInstance />
					</ProCard.TabPane>
					<ProCard.TabPane key="tab2" tab="数据集">
						<StarRocksDataCollection />
					</ProCard.TabPane>
				</ProCard>
			</Drawer>
		</div>

	)
};

export default StarRocksClusterConfig;
