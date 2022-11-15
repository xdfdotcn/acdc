import React, {useEffect, useState} from 'react';
import ProCard from '@ant-design/pro-card';
import RcResizeObserver from 'rc-resize-observer';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import styles from './index.less';
import {queryRdbDatabase, queryRdbTable} from '@/services/a-cdc/api';
import { Input } from 'antd';
import {useModel} from 'umi';

const { Search } = Input;

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
	const onSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
		const { value } = e.target;
		setQueryDatabaseName(value);
	};
	
	useEffect(() => {
		if(applyInfoModel.srcDatabaseName && applyInfoModel.srcDatabaseId) {
			if(applyInfoModel.srcSearchDatabase){
				setQueryDatabaseName(applyInfoModel.srcSearchDatabase);
			}
			setSelectId(applyInfoModel.srcDatabaseId);
		}
	}, [applyInfoModel])
	return (
		<ProTable<API.DatabaseListItem, API.DatabaseQuery>
			onRow={(record) => {
				return {
					onClick: () => {
						setSelectId(record.id);
						setApplyInfoModel({
							...applyInfoModel,
							srcDatabaseId:record.id,
							srcDatabaseName:record.name,
							srcSearchDatabase: queryDatabaseName
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
				search:  <Search defaultValue={queryDatabaseName} value={queryDatabaseName} onChange={onSearchChange}/>
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

	const onSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
		const { value } = e.target;
		setQueryTableName(value);
	};
	const onSearch = (value: string) => {
		setQueryTableName(value);
	};

	useEffect(()=>{
		if(applyInfoModel.srcDataSetId && applyInfoModel.srcDataSetName) {
			if(applyInfoModel.srcSearchDataset){
				setQueryTableName(applyInfoModel.srcSearchDataset);
			}
			setSelectId(applyInfoModel.srcDataSetId);
		}
	}, [applyInfoModel]);
	return (
		<ProTable<API.TableListItem, API.TableQuery>
			onRow={(record) => {
				return {
					onClick: () => {
						setSelectId(record.id);
						setApplyInfoModel({
							...applyInfoModel,
							srcDataSetId: record.id,
							srcDataSetName: record.name,
							srcSearchDataset: queryTableName
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
				search: <Search defaultValue={queryTableName} value={queryTableName} onChange={onSearchChange} onSearch={onSearch}/>
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

const Step2: React.FC = () => {
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

export default Step2;
