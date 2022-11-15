import React, {useEffect, useState} from 'react';
import ProCard from '@ant-design/pro-card';
import RcResizeObserver from 'rc-resize-observer';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import styles from './index.less';
import {queryProject, queryCluster, queryInstance} from '@/services/a-cdc/api';
import {useModel} from 'umi';
import { Input } from 'antd';

const { Search } = Input;

const ProjectList: React.FC = () => {
	const {applyInfoModel,setApplyInfoModel} = useModel('ConnectorApplyModel')
	const projectColumns: ProColumns<API.Project>[] = [
		{
			title: '项目名称',
			dataIndex: 'name',
		},
		{
			title: '项目描述',
			dataIndex: 'description',
			ellipsis: true,
		},
	];

	// 选中
	const [selectId, setSelectId] = useState<number>();
	// 模糊查询
	const [queryProjectName, setQueryProjectName] = useState<string>();

	const onSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
		const { value } = e.target;
		setQueryProjectName(value);
	};
	const onSearch = (value: string) => {
		setQueryProjectName(value);
	}

	useEffect(()=>{
		if(applyInfoModel.sinkPrjId && applyInfoModel.sinkPrjName) {
			if(applyInfoModel.sinkSearchPrj){
				setQueryProjectName(applyInfoModel.sinkSearchPrj);
			}
			setSelectId(applyInfoModel.sinkPrjId);
		}
	},[applyInfoModel]);

	return (
		<ProTable<API.Project>
			onRow={(record) => {
				return {
					onClick: () => {
						setSelectId(record.id);
						setApplyInfoModel({
							...applyInfoModel,
							sinkPrjId:record.id,
							sinkPrjName:record.name,
							sinkSearchPrj: queryProjectName
						})
					},
				};
			}}

			rowClassName={(record) => {
				if (record.id === selectId && applyInfoModel!.sinkPrjId! === selectId) {
					return styles['split-row-select-active']
				}
				return ''
			}}
			params={{
				name: queryProjectName,
				queryRange: 'CURRENT_USER'
			}}
			// 请求数据API
			request={queryProject}
			columns={projectColumns}
			toolbar={{
				search: <Search defaultValue={queryProjectName} value={queryProjectName} onChange={onSearchChange} onSearch={onSearch}/>
			}}
			// 分页设置,默认数据,不展示动态调整分页大小
			pagination={{
				showSizeChanger: false,
				pageSize: 10
			}}
			options={false}
			rowKey={(record) => String(record.id)}
			search={false}
		/>
	)
};

// rdb列表,处理联动
const ClusterList: React.FC = () => {
	const clusterColumns: ProColumns<API.ClusterListItem>[] = [
		{
			title: '集群',
			dataIndex: 'name',
		},
		{
			title: '类型',
			dataIndex: 'dataSystemType'
		},
		//{
			//title: '描述',
			//dataIndex: 'desc'
		//},
	];

	const {applyInfoModel, setApplyInfoModel} = useModel('ConnectorApplyModel')
	// 选中
	const [selectId, setSelectId] = useState<number>();
	// 模糊查询
	const [queryClusterName, setQueryClusterName] = useState<string>();
	
	const onSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
		const { value } = e.target;
		setQueryClusterName(value);
	};
	const onSearch = (value: string) => {
		setQueryClusterName(value);
	};

	useEffect(()=>{
		if(applyInfoModel.sinkClusterId && applyInfoModel.sinkClusterName) {
			if(applyInfoModel.sinkSearchCluster){
				setQueryClusterName(applyInfoModel.sinkSearchCluster);
			}
			
			setSelectId(applyInfoModel.sinkClusterId);
		}
	},[applyInfoModel]);
	return (
		<ProTable<API.ClusterListItem, API.ClusterQuery>
			onRow={(record) => {
				return {
					onClick: () => {
						setSelectId(record.id);
						setApplyInfoModel({
							...applyInfoModel,
							sinkClusterId: record.id,
							sinkDataSystemType: record.dataSystemType,
							sinkClusterType:record.clusterType,
							sinkClusterName: record.name,
							sinkSearchCluster: queryClusterName
						})
					},
				};
			}}
			rowClassName={(record) => {
				if (record.id === selectId && applyInfoModel!.sinkClusterId! === selectId) {
					return styles['split-row-select-active']
				}
				return ''

			}}
			params={{
				projectId: applyInfoModel.sinkPrjId,
				name: queryClusterName
			}}
			request={queryCluster}
			columns={clusterColumns}
			toolbar={{
				search: <Search defaultValue={queryClusterName} value={queryClusterName} onChange={onSearchChange} onSearch={onSearch}/>
			}}
			pagination={{
				showSizeChanger: false,
				pageSize: 100
			}}
			options={false}
			rowKey={(record) => String(record.id)}
			search={false}
		/>
	)
};
//sink instance
const InstList: React.FC = () => {
	const instColumns: ProColumns<API.InstanceListItem>[] = [
		{
			title: '实例名称',
			dataIndex: 'name',
			width:'80%'
		},
		{
			title: '标签',
			dataIndex: 'tag',
			width:'20%'
		},
	];

	const {applyInfoModel, setApplyInfoModel} = useModel('ConnectorApplyModel')
	const [selectId, setSelectId] = useState<number>();
	const [queryInstName, setQueryInstName] = useState<string>();

	const onSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
		const { value } = e.target;
		setQueryInstName(value);
	};
	const onSearch = (value: string) => {
		setQueryInstName(value);
	};

	useEffect(()=>{
		if(applyInfoModel.sinkInstanceId && applyInfoModel.sinkInstanceName) {
			if(applyInfoModel.queryInstName){
				setQueryInstName(applyInfoModel.queryInstName);
			}
			setSelectId(applyInfoModel.sinkInstanceId);
		}
	},[applyInfoModel]);
	return (
		<ProTable<API.InstanceListItem, API.InstanceQuery>
			onRow={(record) => {
				return {
					onClick: () => {
						setSelectId(record.id);
						setApplyInfoModel({
							...applyInfoModel,
							sinkInstanceId: record.id,
							sinkInstanceName: record.name,
							sinkSearchInstance: queryInstName
						})
					},
				};
			}}
			rowClassName={(record) => {
				if (record.id === selectId && applyInfoModel!.sinkInstanceId! === selectId) {
					return styles['split-row-select-active']
				}
				return ''
			}}
			params={{
				clusterId: applyInfoModel.sinkClusterId,
				dataSystemType: applyInfoModel.sinkDataSystemType,
				name: queryInstName
			}}

			request={queryInstance}
			columns={instColumns}
			toolbar={{
					search: <Search defaultValue={queryInstName} value={queryInstName} onChange={onSearchChange} onSearch={onSearch}/>
			}}
			pagination={{
				showSizeChanger: false,
				pageSize: 10
			}}
			options={false}
			rowKey={(record) => String(record.id)}
			search={false}
		/>
	)
};

const Step3: React.FC = () => {
	const [responsive, setResponsive] = useState(false);
	return (
		// main page
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
					<ProCard>
						<ProjectList />
					</ProCard>
					<ProCard >
						<ClusterList />
					</ProCard>
					<ProCard >
						<InstList />
					</ProCard>
				</ProCard>
			</RcResizeObserver>
		</ProCard>
	)
};

export default Step3;
