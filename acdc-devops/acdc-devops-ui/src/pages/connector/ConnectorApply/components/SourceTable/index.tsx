import React, {useState} from 'react';
import ProCard from '@ant-design/pro-card';
import RcResizeObserver from 'rc-resize-observer';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import styles from './split.less';
import {queryRdbDatabase, queryRdbTable} from '@/services/a-cdc/api';

import {useModel} from 'umi';

const DatabaseList: React.FC = () => {

	const {applyInfoModel, setApplyInfoModel} = useModel('ConnectorApplyModel')

	const databaseColumns: ProColumns<API.DatabaseListItem>[] = [
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
		<ProTable<API.DatabaseListItem, API.DatabaseQuery>
			onRow={(record) => {
				return {
					onClick: () => {
						setSelectId(record.id);
						setApplyInfoModel({
							srcPrjId:applyInfoModel.srcPrjId,
							srcPrjName:applyInfoModel.srcPrjName,
							srcDataSystemType:applyInfoModel.srcDataSystemType,
							srcClusterId:applyInfoModel.srcClusterId,
							srcClusterName:applyInfoModel.srcClusterName,
							srcDatabaseId:record.id,
							srcDatabaseName:record.name,
						})
					},
				};
			}}

			rowClassName={(record) => {
				if (record.id === selectId && applyInfoModel!.srcDatabaseId! === selectId) {
					return styles['split-row-select-active']
				}
				return ''
			}}

			params={{
				clusterId: applyInfoModel.srcClusterId,
				dataSystemType:applyInfoModel.srcDataSystemType,
				name: queryDatabaseName
			}}

			request={queryRdbDatabase}

			columns={databaseColumns}

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

	const {applyInfoModel,setApplyInfoModel} = useModel('ConnectorApplyModel')

	const [selectId, setSelectId] = useState<number>();

	const [queryTableName, setQueryTableName] = useState<string>();

	return (
		<ProTable<API.TableListItem, API.TableQuery>
			onRow={(record) => {
				return {
					onClick: () => {
						setSelectId(record.id);
						setApplyInfoModel({
							srcPrjId:applyInfoModel.srcPrjId,
							srcPrjName: applyInfoModel.srcPrjName,
							srcDataSystemType: applyInfoModel.srcDataSystemType,
							srcClusterId: applyInfoModel.srcClusterId,
							srcClusterName: applyInfoModel.srcClusterName,
							srcDatabaseId:applyInfoModel.srcDatabaseId,
							srcDatabaseName:applyInfoModel.srcDatabaseName,
							srcDataSetId: record.id,
							srcDataSetName:record.name,
						})
					},
				};
			}}
			rowClassName={(record) => {
				if (record.id === selectId && applyInfoModel!.srcDataSetId! === selectId) {
					return styles['split-row-select-active']
				}
				return ''
			}}
			params={{
				databaseId: applyInfoModel.srcDatabaseId,
				dataSystemType:applyInfoModel.srcDataSystemType,
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
