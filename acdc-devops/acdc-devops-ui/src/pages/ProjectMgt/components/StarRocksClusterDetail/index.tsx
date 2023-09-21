import React, {useEffect, useState} from 'react';
import {useModel} from 'umi';
import {Descriptions} from 'antd';
import ProCard from '@ant-design/pro-card';
import {getDataSystemResource} from '@/services/a-cdc/api';
const StarRocksClusterDetail: React.FC = () => {
	// 项目详情页面的数据model
	const {starRocksClusterDetailModel} = useModel('StarRocksClusterDetailModel')
	const [starRocksClusterDetail, setStarRocksClusterDetail] = useState<API.StarRocks>({});

	useEffect(() => {
		initData();
	}, [starRocksClusterDetailModel.resourceId]);

	const initData = async () => {
		let starRocks: API.DataSystemResource = await getDataSystemResource({id: starRocksClusterDetailModel.resourceId})
		setStarRocksClusterDetail({
			id: starRocks.id,
			name: starRocks!.name,
      description: starRocks!.description,
			username: starRocks!.name,
		})
	}

	return (
		<div>
			<ProCard
				title="集群信息"
				headerBordered
				onCollapse={(collapse) => {}}
			>
				<Descriptions column={1}>
					<Descriptions.Item label="集群名称">
						{starRocksClusterDetail.name}
					</Descriptions.Item>
					<Descriptions.Item label="类型">
						STARROCKS
					</Descriptions.Item>
					<Descriptions.Item label="描述">
						{starRocksClusterDetail.description}
					</Descriptions.Item>
				</Descriptions>
			</ProCard>
		</div>
	)
};
export default StarRocksClusterDetail;
