import React, {useEffect, useState} from 'react';
import {useModel} from 'umi';
import {Descriptions} from 'antd';
import ProCard from '@ant-design/pro-card';
import {getDataSystemResource} from '@/services/a-cdc/api';
const RdbClusterDetail: React.FC = () => {
	// 项目详情页面的数据model
	const {rdbClusterDetailModel} = useModel('RdbClusterDetailModel')
	const [rdbClusterDetail, setRdbClusterDetail] = useState<API.Rdb>({});

	useEffect(() => {
		initData();
	}, [rdbClusterDetailModel.resourceId]);

	const initData = async () => {
		let rdb: API.DataSystemResource = await getDataSystemResource({id: rdbClusterDetailModel.resourceId})
		setRdbClusterDetail({
			id: rdb.id,
			name: rdb!.name,
      		description: rdb!.description,
			rdbType: rdb!.dataSystemType,
			username: rdb!.name,
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
						{rdbClusterDetail.name}
					</Descriptions.Item>
					<Descriptions.Item label="类型">
						{rdbClusterDetail.rdbType}
					</Descriptions.Item>
					<Descriptions.Item label="描述">
						{rdbClusterDetail.description}
					</Descriptions.Item>
				</Descriptions>
			</ProCard>
		</div>
	)
};
export default RdbClusterDetail;
