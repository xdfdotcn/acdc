import React, {useRef, useState} from 'react';
import ProCard from '@ant-design/pro-card';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable, {ActionType} from '@ant-design/pro-table';
import {
  pagedQueryDataSystemResource,
  refreshDynamicDataSystemResource
} from '@/services/a-cdc/api';

import {useModel} from 'umi';
import {Button} from "antd";
import {SyncOutlined} from "@ant-design/icons";
import { DataSystemResourceTypeConstant } from '@/services/a-cdc/constant/DataSystemResourceTypeConstant';

const EsDataSetList: React.FC = () => {

	const {esDatasetModel} = useModel('EsDatasetModel')

  const [refresh, setRefresh] = useState(false);

	const columns: ProColumns<API.DataSystemResource>[] = [
		{
			title: 'Index 名称',
			dataIndex: 'name',
		}
	];

	// 模糊搜索
	const [queryDatasetName, setQueryDatasetName] = useState<string>();

  const ref = useRef<ActionType>();

	return (
		<ProTable<API.DataSystemResource, API.DataSystemResourceQuery>
			params={{
        parentResourceId: esDatasetModel.esClusterId,
				name: queryDatasetName,
        resourceTypes: [DataSystemResourceTypeConstant.ELASTICSEARCH_INDEX]
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
              await refreshDynamicDataSystemResource(esDatasetModel.esClusterId)
              ref.current?.reload();
              setRefresh(false)
            }}>
            刷新 Index
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
			<EsDataSetList />
		</ProCard>
	)
};

export default MainPage;
