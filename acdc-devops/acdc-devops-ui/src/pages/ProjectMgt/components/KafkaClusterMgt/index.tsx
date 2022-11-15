import React, {useState} from 'react';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import {Modal, Button} from 'antd';
import {EditOutlined, PlusOutlined} from '@ant-design/icons';
import {useModel} from 'umi';
import {queryKafkaCluster} from '@/services/a-cdc/api';
import KafkaClusterEditing from '../KafkaClusterEditing';
import KafkaClusterConfig from '../KafkaClusterConfig';
const {confirm} = Modal;

const KafkaClusterMgt: React.FC = () => {
	// 数据流
	const {kafkaClusterEditingModel, setKafkaClusterEditingModel} = useModel('KafkaClusterEditingModel')
	const {kafkaClusterConfigModel, setKafkaClusterConfigModel} = useModel('KafkaClusterConfigModel')
	const {kafkaClusterMgtModel, setKafkaClusterMgtModel} = useModel('KafkaClusterMgtModel')
	const {kafkaClusterDetailModel, setKafkaClusterDetailModel} = useModel('KafkaClusterDetailModel')
	const columns: ProColumns<API.KafkaCluster>[] = [
		{
			title: '名称',
			width: "30%",
			dataIndex: 'name',
		},

		{
			title: '集群地址',
			width: "30%",
			dataIndex: 'bootstrapServers',
			search:false,
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
			<ProTable<API.KafkaCluster>
				rowKey={(record) => String(record.id)}
				// 请求数据API
				columns={columns}
				// 分页设置,默认数据,不展示动态调整分页大小
				params={{
					projectId: kafkaClusterMgtModel.projectId
				}}
				request={queryKafkaCluster}
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
			<KafkaClusterEditing />
			<KafkaClusterConfig />
		</div>
	)
};

export default KafkaClusterMgt;
