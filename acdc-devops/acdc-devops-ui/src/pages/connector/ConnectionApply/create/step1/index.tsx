import React, {useEffect, useState} from 'react';
import ProCard from '@ant-design/pro-card';
import RcResizeObserver from 'rc-resize-observer';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import styles from './index.less';
import {queryProject, queryCluster} from '@/services/a-cdc/api';
import {useModel} from 'umi';
import { Input } from 'antd';
import { debounce } from 'lodash';

const { Search } = Input;

//项目列表
const ProjectList: React.FC = () => {

	const {applyInfoModel, setApplyInfoModel} = useModel('ConnectorApplyModel')
	
	const projectColumns: ProColumns<API.Project>[] = [
		{
			title: '项目名称',
			dataIndex: 'name',
		},
		{
			title: '负责人',
			dataIndex: 'ownerEmail',
		},
		{
			title: '项目描述',
			dataIndex: 'description',
			ellipsis: true,
		},
	];

	// 选中
	const [selectItem, setSelectItem] = useState<{ srcPrjId?: number, srcPrjName?: string }>();
	
	// 模糊搜索
	const [projectName, setProjectName] = useState<string>();
	
	const onSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
		const { value } = e.target;
		setProjectName(value);
	};
	const onSearch = (value: string) => {
		setProjectName(value);
	};

	useEffect(()=>{
		if(applyInfoModel.srcPrjId){
			if(applyInfoModel.srcSearchPrj) {
				setProjectName(applyInfoModel.srcSearchPrj);
			}
			setSelectItem({
				srcPrjId: applyInfoModel.srcPrjId,
				srcPrjName: applyInfoModel.srcPrjName,
			})
		}
	},[applyInfoModel])
	return (
		<ProTable<API.Project>
			// rowSelection={{
			// 	type: 'radio',
			// 	selections: [Table.SELECTION_ALL, Table.SELECTION_INVERT],
			// 	defaultSelectedRowKeys: [1],
			// }}
			onRow={(record) => {
				return {
					onClick: () => {
						setSelectItem({ srcPrjId: record.id, srcPrjName:record.name });
						// 保存选中项目id
						setApplyInfoModel({
							srcPrjId: record.id,
							srcPrjName: record.name,
							srcSearchPrj: projectName,
							srcPrjOwnerEmail:record.ownerEmail,
						})
					},
				};
			}}

			// 页面重新渲染,选择的item进行高亮
			rowClassName={(record) => {
				return record.id === selectItem?.srcPrjId ? styles['split-row-select-active'] : '';
			}}

			// 增加次属性是为了页面重新渲染触发 request重新请求数据
			params={{
				name: projectName,
				queryRange: 'ALL'
			}}
			// 请求数据API
			request={queryProject}
			columns={projectColumns}
			toolbar={{
				search: <Search defaultValue={projectName} value={projectName} onChange={onSearchChange} onSearch={onSearch}/>
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


// 集群列表
const ClusterList: React.FC = () => {

	const {applyInfoModel, setApplyInfoModel} = useModel('ConnectorApplyModel')

	const clusterColumns: ProColumns<API.ClusterListItem>[] = [
		{
			title: '集群',
			dataIndex: 'name',
		},
		{
			title: '类型',
			dataIndex: 'dataSystemType'
		},
		{
			title: '描述',
			dataIndex: 'desc'
		},
	];

	const [selectItem, setselectItem] = useState<{
		srcClusterId?: number,
		srcDataSystemType?: string,
		srcClusterName?: string,
		srcClusterType?: string,
	}>();

	const [queryClusterName, setQueryClusterName] = useState<string>();
	const onSearchChange =  (e: React.ChangeEvent<HTMLInputElement>) => {
		const { value } = e.target;
		setQueryClusterName(value);
	};
	const onSearch = (value: string) => {
		setQueryClusterName(value);
	}
	useEffect(() => {
		if( applyInfoModel.srcClusterName ){
			if(applyInfoModel.srcSearchCluster) {
				setQueryClusterName(applyInfoModel.srcSearchCluster)
			}
			setselectItem({
				srcClusterId: applyInfoModel.srcClusterId,
				srcDataSystemType: applyInfoModel.srcDataSystemType,
				srcClusterName: applyInfoModel.srcClusterName,
				srcClusterType: applyInfoModel.srcClusterType,
			})
		}
	
		// 上级选择节点发生变化,触发UI重新渲染
		//const projectId = applyInfoModel?.srcPrjId;
		/**
			条件控制的原因:

			1. 这个钩子函数的触发时机是页面重新渲染完

			2. 这个钩子里面有更改状态的操作,更改全局数据流中对应的ID值

			3. 如果不加入条件判断,直接更改全局数据流中的值,会导致所有页面重新渲染
			出现死循环

			4. 更改全局数据流中的数据状态,是因为父节点改变,如果不把对应的id失效,点击下
			一步的时候,会有上一次选择缓存的ID,下一步校验的时候不能判断子节点是否做了重
			新选中
		*/
		//if (queryId != projectId) {

			//[>*
				//触发联动查询,可能会出现undefined的id,这个情况出现在页面刷新,三级联动,父
				//节点没有做任何的选中
			//*/
			//setQueryId(projectId)

			//// 失效选中,触发选中样式重新渲染
			//setselectItem(undefined)

			//// 更改全局数据流状态,下一步校验,因父节点选中发生改变,子节点上一次的选中需要失效
			//setSourceRdbIdAndRdbType(undefined, undefined)
		//}
	},[applyInfoModel]);
	return (
		<ProTable<API.ClusterListItem, API.ClusterQuery>
			onRow={(record) => {
				return {
					onClick: () => {
						setselectItem({
							srcClusterId: record.id,
							srcDataSystemType:record.dataSystemType,
							srcClusterName:record.name,
							srcClusterType:record.clusterType,
						})
						setApplyInfoModel({
							...applyInfoModel,
							srcClusterId: record.id,
							srcDataSystemType:record.dataSystemType,
							srcClusterName:record.name,
							srcClusterType:record.clusterType,
							srcSearchCluster: queryClusterName
						})
					},
				};
			}}

			rowClassName={(record) => 
				// && applyInfoModel!.srcClusterId! === selectItem.srcClusterId
				record.id === selectItem?.srcClusterId ? styles['split-row-select-active'] : ''
			}

			params={{
				projectId: applyInfoModel.srcPrjId,
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

const Step1: React.FC = () => {

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
						{/**记录project列表选择的id,更新到mainPage的状态信息中*/}
						<ProjectList />
					</ProCard>
					<ProCard >
						{/**根据projectId 触发联动查询*/}
						<ClusterList />
					</ProCard>
				</ProCard>
			</RcResizeObserver>
		</ProCard>
	)
};

export default Step1;
