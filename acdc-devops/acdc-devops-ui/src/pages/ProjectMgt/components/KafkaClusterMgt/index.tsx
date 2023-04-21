import React, {useRef} from 'react';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable, {ActionType} from '@ant-design/pro-table';
import {Button} from 'antd';
import {EditOutlined, PlusOutlined} from '@ant-design/icons';
import {useModel} from 'umi';
import {pagedQueryDataSystemResource} from '@/services/a-cdc/api';
import KafkaClusterEditing from '../KafkaClusterEditing';
import KafkaClusterConfig from '../KafkaClusterConfig';

const KafkaClusterMgt: React.FC = () => {
	// 数据流
	const {kafkaClusterEditingModel, setKafkaClusterEditingModel} = useModel('KafkaClusterEditingModel')
	const {kafkaClusterConfigModel, setKafkaClusterConfigModel} = useModel('KafkaClusterConfigModel')
	const {kafkaClusterMgtModel} = useModel('KafkaClusterMgtModel')

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
			dataIndex: ['dataSystemResourceConfigurations', 'bootstrap.servers', 'value'],
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
						setKafkaClusterEditingModel({
							...kafkaClusterEditingModel,
							kafkaClusterId: record.id,
							projectId: kafkaClusterMgtModel.projectId,
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
						setKafkaClusterConfigModel({
							...kafkaClusterConfigModel,
							kafkaClusterId: record.id,
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
					projectId: kafkaClusterMgtModel.projectId,
          resourceTypes: ["KAFKA_CLUSTER"]
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
								setKafkaClusterEditingModel({
									...kafkaClusterEditingModel,
									showModal: true,
									projectId:kafkaClusterMgtModel.projectId,
									from:'create'
								})
							}}>
							新增
						</Button>
					</Button.Group>
				]}
			/>
			<KafkaClusterEditing tableRef={ref.current} />
			<KafkaClusterConfig />
		</div>
	)
};

export default KafkaClusterMgt;
