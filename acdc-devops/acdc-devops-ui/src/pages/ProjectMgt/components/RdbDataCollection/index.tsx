import React, {useRef, useState} from 'react';
import ProCard from '@ant-design/pro-card';
import RcResizeObserver from 'rc-resize-observer';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable, {ActionType} from '@ant-design/pro-table';
import styles from './split.less';
import {
  pagedQueryDataSystemResource,
  refreshDynamicDataSystemResource
} from '@/services/a-cdc/api';

import {useModel} from 'umi';
import {Button} from "antd";
import {SyncOutlined} from "@ant-design/icons";
import {DataSystemResourceTypeConstant} from "@/services/a-cdc/constant/DataSystemResourceTypeConstant";
import {DataSystemTypeConstant} from "@/services/a-cdc/constant/DataSystemTypeConstant";

const DatabaseList: React.FC = () => {

	const {rdbDatasetModel, setRdbDatasetModel} = useModel('RdbDatasetModel')

  const [refresh, setRefresh] = useState(false);

	const columns: ProColumns<API.DataSystemResource>[] = [
		{
			title: '数据库名',
			dataIndex: 'name',
		}
	];

	// 选中
	const [selectId, setSelectId] = useState<number>();

	// 模糊搜索
	const [queryDatabaseName, setQueryDatabaseName] = useState<string>();

  const ref = useRef<ActionType>();

	return (
      <ProTable<API.DataSystemResource, API.DataSystemResourceQuery>
        onRow={(record) => {
          return {
            onClick: () => {
              setSelectId(record.id);
              setRdbDatasetModel({
                ...rdbDatasetModel,
                clusterResourceId: rdbDatasetModel.clusterResourceId,
                databaseResourceId: record.id
              })
            },
          };
        }}

        actionRef={ref}

        rowClassName={(record) => {
          if (record.id === selectId) {
            return styles['split-row-select-active']
          }
          return ''
        }}

        params={{
          parentResourceId: rdbDatasetModel.clusterResourceId,
          resourceTypes: rdbDatasetModel.dataSystemType == DataSystemTypeConstant.MYSQL
            ? [DataSystemResourceTypeConstant.MYSQL_DATABASE] : [DataSystemResourceTypeConstant.TIDB_DATABASE],
          name: queryDatabaseName
        }}

        request={pagedQueryDataSystemResource}

        columns={columns}

        toolbar={{
          search: {
            onSearch: (value: string) => {
              setQueryDatabaseName(value);
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
                await refreshDynamicDataSystemResource(rdbDatasetModel.clusterResourceId)
                ref.current?.reload();
                setRefresh(false)
              }}>
              刷新 database + table
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


const TableList: React.FC = () => {
	const tableColumns: ProColumns<API.DataSystemResource>[] = [
		{
			title: '数据表名',
			dataIndex: 'name',
		}
	];

	const {rdbDatasetModel} = useModel('RdbDatasetModel')

	const [selectId, setSelectId] = useState<number>();

	const [queryTableName, setQueryTableName] = useState<string>();

	return (
		<ProTable<API.DataSystemResource, API.DataSystemResourceQuery>
			onRow={(record) => {
				return {
					onClick: () => {
						setSelectId(record.id);
					},
				};
			}}
			rowClassName={(record) => {
				if (record.id === selectId) {
					return styles['split-row-select-active']
				}
				return ''
			}}
			params={{
        parentResourceId: rdbDatasetModel.databaseResourceId,
				name: queryTableName
			}}
			request={pagedQueryDataSystemResource}
			columns={tableColumns}
			toolbar={{
				search: {
					onSearch: (value: string) => {
						setQueryTableName(value);
					},
				},
			}}
      options={{
        density: false,
        fullScreen: false,
        reload: true,
        setting: false
      }}
			rowKey={(record) => String(record.id)}
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
				marginBottom: 16,
				minWidth: 800,
				maxWidth: '100%',
			}}>

			<RcResizeObserver
				key="resize-observer"
				onResize={(offset) => {
					setResponsive(offset.width < 100);
				}}
			>
				<ProCard bordered split={responsive ? 'horizontal' : 'vertical'} >
					<ProCard colSpan="50%">
						<DatabaseList />
					</ProCard>
					<ProCard colSpan="50%" >
						<TableList />
					</ProCard>
				</ProCard>
			</RcResizeObserver>
		</ProCard>
	)
};

export default MainPage;
