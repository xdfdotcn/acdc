import React, {useEffect, useState} from 'react';
import {useModel} from 'umi';
import {Descriptions} from 'antd';
import ProCard from '@ant-design/pro-card';
import {getDataSystemResource} from '@/services/a-cdc/api';
const EsClusterDetail: React.FC = () => {

	const {esClusterDetailModel} = useModel('EsClusterDetailModel')

	const [esClusterDetail, setEsClusterDetail] = useState<API.EsCluster>({});

	useEffect(() => {
		initData();
	}, [esClusterDetailModel.esClusterId]);

	const initData = async () => {
    if (!esClusterDetailModel.esClusterId) {
      return
    }

		let esCluster: API.DataSystemResource = await getDataSystemResource({id: esClusterDetailModel.esClusterId})
		setEsClusterDetail({
      id: esCluster.id,
      name: esCluster.name,
      nodeServers: esCluster.dataSystemResourceConfigurations["node.servers"].value,
      description: esCluster.description,
      username: esCluster.dataSystemResourceConfigurations["username"] ? esCluster.dataSystemResourceConfigurations["username"].value : ""
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
						{esClusterDetail.name}
					</Descriptions.Item>
					<Descriptions.Item label="集群地址">
						{esClusterDetail.nodeServers}
					</Descriptions.Item>
					<Descriptions.Item label="username">
						{esClusterDetail.username}
					</Descriptions.Item>
				</Descriptions>
			</ProCard>
		</div>
	)
};
export default EsClusterDetail;
