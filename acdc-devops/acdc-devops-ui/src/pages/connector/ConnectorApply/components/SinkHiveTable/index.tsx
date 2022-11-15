import React, {useEffect, useState} from 'react';
import ProCard from '@ant-design/pro-card';
import RcResizeObserver from 'rc-resize-observer';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import styles from './split.less';
import {queryHiveDatabase, queryHiveTable, queryRdbDatabase, queryRdbTable} from '@/services/a-cdc/api';
import {useModel} from 'umi';
const DatabaseList: React.FC = () => {
	const databaseColumns: ProColumns<API.DatabaseListItem>[] = [
		{
			title: 'Hive库名',
			dataIndex: 'name',
		}
	];

	const {applyInfoModel, setApplyInfoModel} = useModel('ConnectorApplyModel')

	// 选中
	const [selectId, setSelectId] = useState<number>();

	// 模糊查询
	const [queryDatabaseName, setQueryDatabaseName] = useState<string>();

	return (
		<ProTable<API.DatabaseListItem, API.DatabaseQuery>
			onRow={(record) => {
				return {
					onClick: () => {
						setSelectId(record.id);
						setApplyInfoModel({
							srcPrjId: applyInfoModel.srcPrjId,
							srcPrjName: applyInfoModel.srcPrjName,
							srcDataSystemType: applyInfoModel.srcDataSystemType,
							srcClusterId: applyInfoModel.srcClusterId,
							srcClusterName: applyInfoModel.srcClusterName,
							srcDatabaseId: applyInfoModel.srcDatabaseId,
							srcDatabaseName: applyInfoModel.srcDatabaseName,
							srcDataSetId: applyInfoModel.srcDataSetId,
							srcDataSetName: applyInfoModel.srcDataSetName,
							sinkPrjId: applyInfoModel.sinkPrjId,
							sinkPrjName: applyInfoModel.sinkPrjName,
							sinkClusterId: applyInfoModel.sinkClusterId,
							sinkDataSystemType: applyInfoModel.sinkDataSystemType,
							sinkClusterName: applyInfoModel.sinkClusterName,
							sinkClusterType:applyInfoModel.sinkClusterType,
							sinkInstanceId: applyInfoModel.sinkInstanceId,
							sinkInstanceName: applyInfoModel.sinkInstanceName,
							sinkDatabaseId: record.id,
							sinkDatabaseName: record.name,
						})
					},
				};
			}}

			rowClassName={(record) => {
				if (record.id === selectId && applyInfoModel!.sinkDatabaseId! === selectId) {
					return styles['split-row-select-active']
				}
				return ''
			}}

			params={{
				clusterId: applyInfoModel.sinkClusterId,
				name: queryDatabaseName,
			}}

			request={queryHiveDatabase}

			columns={databaseColumns}

			toolbar={{
				search: {
					onSearch: (value: string) => {
						setQueryDatabaseName(value);
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


const TableList: React.FC = () => {
	const tableColumns: ProColumns<API.TableListItem>[] = [
		{
			title: 'Hive表名',
			dataIndex: 'name',
		}
	];

	const {applyInfoModel, setApplyInfoModel} = useModel('ConnectorApplyModel')

	// 选中
	const [selectId, setSelectId] = useState<number>();

	// 模糊查询
	const [queryTableName, setQueryTableName] = useState<string>();

	return (
		<ProTable<API.TableListItem, API.TableQuery>
			onRow={(record) => {
				return {
					onClick: () => {
						setSelectId(record.id);
						setApplyInfoModel({
							srcPrjId: applyInfoModel.srcPrjId,
							srcPrjName: applyInfoModel.srcPrjName,
							srcDataSystemType: applyInfoModel.srcDataSystemType,
							srcClusterId: applyInfoModel.srcClusterId,
							srcClusterName: applyInfoModel.srcClusterName,
							srcDatabaseId: applyInfoModel.srcDatabaseId,
							srcDatabaseName: applyInfoModel.srcDatabaseName,
							srcDataSetId: applyInfoModel.srcDataSetId,
							srcDataSetName: applyInfoModel.srcDataSetName,
							sinkPrjId: applyInfoModel.sinkPrjId,
							sinkPrjName: applyInfoModel.sinkPrjName,
							sinkClusterId: applyInfoModel.sinkClusterId,
							sinkDataSystemType: applyInfoModel.sinkDataSystemType,
							sinkClusterName: applyInfoModel.sinkClusterName,
							sinkClusterType:applyInfoModel.sinkClusterType,
							sinkInstanceId: applyInfoModel.sinkInstanceId,
							sinkInstanceName: applyInfoModel.sinkInstanceName,
							sinkDatabaseId:applyInfoModel.sinkDatabaseId,
							sinkDatabaseName:applyInfoModel.sinkDatabaseName,
							sinkDataSetId:record.id,
							sinkDataSetName:record.name,
						})
					},
				};
			}}
			rowClassName={(record) => {
				if (record.id === selectId && applyInfoModel!.sinkDataSetId! === selectId) {
					return styles['split-row-select-active']
				}
				return ''
			}}
			params={{
				databaseId: applyInfoModel.sinkDatabaseId,
				name: queryTableName
			}}
			request={queryHiveTable}
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
	return (
		<ProCard
			title="选择数据库"
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
