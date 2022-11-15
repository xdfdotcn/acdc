import React, {useEffect, useRef, useState} from 'react';
import {useModel} from 'umi';
import {Descriptions, Modal} from 'antd';
import ProCard from '@ant-design/pro-card';
import {getRdb} from '@/services/a-cdc/api';
const RdbClusterDetail: React.FC = () => {
	// 项目详情页面的数据model
	const {rdbClusterDetailModel} = useModel('RdbClusterDetailModel')
	const [rdbClusterDetail, setRdbClusterDetail] = useState<API.Rdb>({});

	useEffect(() => {
		initData();
	}, [rdbClusterDetailModel.rdbId]);


	const initData = async () => {
		let rdb: API.Rdb = await getRdb({rdbId: rdbClusterDetailModel.rdbId})
		setRdbClusterDetail({
			id: rdbClusterDetailModel.rdbId,
			name: rdb!.name,
			desc: rdb!.desc,
			rdbType: rdb!.rdbType,
			username: rdb!.username,
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
					<Descriptions.Item label="用户名">
						{rdbClusterDetail.username}
					</Descriptions.Item>
					<Descriptions.Item label="描述">
						{rdbClusterDetail.desc}
					</Descriptions.Item>
				</Descriptions>
			</ProCard>
		</div>
	)
};
export default RdbClusterDetail;
