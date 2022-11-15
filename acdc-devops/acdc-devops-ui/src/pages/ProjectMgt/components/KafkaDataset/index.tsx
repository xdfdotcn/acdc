import React, {useState} from 'react';
import ProCard from '@ant-design/pro-card';
import RcResizeObserver from 'rc-resize-observer';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import styles from './split.less';
import {queryKafkaTopics, queryRdbDatabase, queryRdbTable} from '@/services/a-cdc/api';

import {useModel} from 'umi';

const KafkaDataSetList: React.FC = () => {

	const {kafkaDatasetModel, setKafkaDatasetModel} = useModel('KafkaDatasetModel')

	const columns: ProColumns<API.RdbDatabase>[] = [
		{
			title: 'Topic 名称',
			dataIndex: 'name',
		}
	];

	// 模糊搜索
	const [queryDatasetName, setQueryDatasetName] = useState<string>();

	return (
		<ProTable<API.KafkaTopic>
			params={{
				kafkaClusterId: kafkaDatasetModel.kafkaClusterId,
				name: queryDatasetName
			}}

			request={queryKafkaTopics}

			columns={columns}

			toolbar={{
				search: {
					onSearch: (value: string) => {
						setQueryDatasetName(value);
					},
				},
			}}

			options={false}
			rowKey={(record)=>String(record.id)}
			search={false}
			pagination={{
				showSizeChanger: false,
				pageSize: 10
			}}
		/>
	)
};

// 主页面
const MainPage: React.FC = () => {
	const [responsive, setResponsive] = useState(false);
	// 联动查询,父组件刷新触发子组件刷新
	return (
		<ProCard
			bordered
			hoverable
			headerBordered
			style={{
				maxWidth: '100%',
				minHeight: '100%'
			}}
			>
			<KafkaDataSetList />
		</ProCard>
	)
};

export default MainPage;
