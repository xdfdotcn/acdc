import React, {useRef} from 'react';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable, {ActionType} from '@ant-design/pro-table';
import {Button} from 'antd';
import {EditOutlined, PlusOutlined} from '@ant-design/icons';
import {useModel} from 'umi';
import {pagedQueryDataSystemResource} from '@/services/a-cdc/api';
import EsClusterEditing from '../EsClusterEditing';
import EsClusterConfig from '../EsClusterConfig';
import { DataSystemResourceTypeConstant } from '@/services/a-cdc/constant/DataSystemResourceTypeConstant';

const EsClusterMgt: React.FC = () => {
	// 数据流
	const {esClusterEditingModel, setEsClusterEditingModel} = useModel('EsClusterEditingModel')
	const {esClusterConfigModel, setEsClusterConfigModel} = useModel('EsClusterConfigModel')
	const {esClusterMgtModel} = useModel('EsClusterMgtModel')

  const ref = useRef<ActionType>();

	const columns: ProColumns<API.DataSystemResource>[] = [
		{
			title: '名称',
			width: "30%",
			dataIndex: 'name',
		},

		{
			title: '集群地址',
			width: "30%",
			dataIndex: ['dataSystemResourceConfigurations', 'node.servers', 'value'],
			search: false,
		},

		{
			title: '描述',
			width: "20%",
			dataIndex: 'description',
			search: false,
		},

		{
			title: '操作',
			width: "20%",
			valueType: 'option',
			dataIndex: 'option',
			render: (text, record, _, action) => [
				<a
					key={"editable" + record.id}
					onClick={() => {
						setEsClusterEditingModel({
							...esClusterEditingModel,
							esClusterId: record.id,
							projectId: esClusterMgtModel.projectId,
							showModal: true,
							from: 'edit'
						})
					}}
				>
					<EditOutlined />
					编辑
				</a>,
				<a
					key={"config" + record.id}
					onClick={() => {
						setEsClusterConfigModel({
							...esClusterConfigModel,
							esClusterId: record.id,
							showDrawer: true
						})
					}}
				>
					<EditOutlined />
					集群配置
				</a>
			],
		},
	];
	return (
		<div>
			<ProTable<API.DataSystemResource, API.DataSystemResourceQuery>
				rowKey={(record) => String(record.id)}
				// 请求数据API
				columns={columns}
				// 分页设置,默认数据,不展示动态调整分页大小
				params={{
					projectId: esClusterMgtModel.projectId,
          resourceTypes: [DataSystemResourceTypeConstant.ELASTIC_SEARCH_CLUSTER]
				}}
				request={pagedQueryDataSystemResource}
        actionRef={ref}
				pagination={{
					showSizeChanger: true,
					pageSize: 10
				}}
				options={{
					setting: {
						listsHeight: 400,
					},
				}}
				toolBarRender={() => [
					<Button.Group key="refs" style={{display: 'block'}}>
						<Button
							key="button"
							icon={<PlusOutlined />}
							onClick={() => {
								setEsClusterEditingModel({
									...esClusterEditingModel,
									showModal: true,
									projectId:esClusterMgtModel.projectId,
									from:'create'
								})
							}}>
							新增
						</Button>
					</Button.Group>
				]}
			/>
			<EsClusterEditing tableRef={ref.current} />
			<EsClusterConfig />
		</div>
	)
};

export default EsClusterMgt;
