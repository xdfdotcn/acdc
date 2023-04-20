import React, {useRef, useState} from 'react';
import ProCard from '@ant-design/pro-card';
import RcResizeObserver from 'rc-resize-observer';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable, {ActionType} from '@ant-design/pro-table';
import styles from './split.less';
import {
  pagedQueryDataSystemResource,
  queryKafkaTopics,
  queryRdbDatabase,
  queryRdbTable,
  refreshDynamicDataSystemResource
} from '@/services/a-cdc/api';

import {useModel} from 'umi';
import {Button} from "antd";
import {SyncOutlined} from "@ant-design/icons";

const KafkaDataSetList: React.FC = () => {

	const {kafkaDatasetModel, setKafkaDatasetModel} = useModel('KafkaDatasetModel')

  const [refresh, setRefresh] = useState(false);

	const columns: ProColumns<API.DataSystemResource>[] = [
		{
			title: 'Topic 名称',
			dataIndex: 'name',
		}
	];

	// 模糊搜索
	const [queryDatasetName, setQueryDatasetName] = useState<string>();

  const ref = useRef<ActionType>();

	return (
		<ProTable<API.DataSystemResource, API.DataSystemResourceQuery>
			params={{
        parentResourceId: kafkaDatasetModel.kafkaClusterId,
				name: queryDatasetName,
        resourceTypes: ["KAFKA_TOPIC"]
			}}

			request={pagedQueryDataSystemResource}

			columns={columns}

      actionRef={ref}

			toolbar={{
				search: {
					onSearch: (value: string) => {
						setQueryDatasetName(value);
					},
				},
			}}

      options={{
        density: false,
        fullScreen: false,
        reload: true,
        setting: false
      }}
      toolBarRender={() => [
        <Button.Group key="refs" style={{display: 'block'}}>
          <Button
            key="button"
            disabled={refresh}
            icon={<SyncOutlined />}
            onClick={async () => {
              setRefresh(true)
              await refreshDynamicDataSystemResource(kafkaDatasetModel.kafkaClusterId)
              ref.current?.reload();
              setRefresh(false)
            }}>
            刷新 Topic
          </Button>
        </Button.Group>
      ]}

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
