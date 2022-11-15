import React, {useState} from 'react';
import ProCard from '@ant-design/pro-card';
import RcResizeObserver from 'rc-resize-observer';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import styles from './split.less';
import {queryRdbDatabase, queryRdbTable} from '@/services/a-cdc/api';

import {useModel} from 'umi';

const DatabaseList: React.FC = () => {

	const {rdbDatasetModel, setRdbDatasetModel} = useModel('RdbDatasetModel')

	const columns: ProColumns<API.RdbDatabase>[] = [
		{
			title: '数据库名',
			dataIndex: 'name',
		}
	];

	// 选中
	const [selectId, setSelectId] = useState<number>();

	// 模糊搜索
	const [queryDatabaseName, setQueryDatabaseName] = useState<string>();

	return (
		<ProTable<API.RdbDatabase>
			onRow={(record) => {
				return {
					onClick: () => {
						setSelectId(record.id);
						setRdbDatasetModel({
							rdbId: rdbDatasetModel.rdbId,
							databaseId: record.id
						})
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
				clusterId: rdbDatasetModel.rdbId,
				name: queryDatabaseName
			}}

			request={queryRdbDatabase}

			columns={columns}

			toolbar={{
				search: {
					onSearch: (value: string) => {
						setQueryDatabaseName(value);
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


const TableList: React.FC = () => {
	const tableColumns: ProColumns<API.TableListItem>[] = [
		{
			title: '数据表名',
			dataIndex: 'name',
		}
	];

	const {rdbDatasetModel, setRdbDatasetModel} = useModel('RdbDatasetModel')

	const [selectId, setSelectId] = useState<number>();

	const [queryTableName, setQueryTableName] = useState<string>();

	return (
		<ProTable<API.RdbTable>
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
				databaseId: rdbDatasetModel.databaseId,
				name: queryTableName
			}}
			request={queryRdbTable}
			columns={tableColumns}
			toolbar={{
				search: {
					onSearch: (value: string) => {
						setQueryTableName(value);
					},
				},
			}}
			options={false}
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
