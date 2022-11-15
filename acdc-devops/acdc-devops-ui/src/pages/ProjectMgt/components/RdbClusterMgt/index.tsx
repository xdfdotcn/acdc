import React, {useState} from 'react';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import {Modal, Button} from 'antd';
import {EditOutlined, PlusOutlined} from '@ant-design/icons';
import {useModel} from 'umi';
import {queryProjectRdb, queryRdb} from '@/services/a-cdc/api';
import RdbClusterEditing from '../RdbClusterEditing';
import RdbClusterConfig from '../RdbClusterConfig';
const {confirm} = Modal;

const RdbClusterMgt: React.FC = () => {
	// 数据流
	const {rdbClusterEditingModel, setRdbClusterEditingModel} = useModel('RdbClusterEditingModel')
	const {rdbClusterConfigModel, setRdbClusterConfigModel} = useModel('RdbClusterConfigModel')
	const {rdbClusterMgtModel, setRdbClusterMgtModel} = useModel('RdbClusterMgtModel')
	const {rdbClusterDetailModel, setRdbClusterDetailModel} = useModel('RdbClusterDetailModel')
	const {rdbInstanceModel, setRdbInstanceModel} = useModel('RdbInstanceModel')
	const columns: ProColumns<API.Rdb>[] = [
		{
			title: '名称',
			width: "40%",
			dataIndex: 'name',
		},

		{
			title: '描述',
			width: "40%",
			dataIndex: 'desc',
			search:false,
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
						setRdbClusterEditingModel({
							...rdbClusterEditingModel,
							showDrawer: true,
							projectId: rdbClusterMgtModel.projectId,
							rdbId: record.id,
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
						setRdbClusterConfigModel({
							...rdbClusterConfigModel,
							showDrawer: true,
							rdbId: record.id,
						})

						setRdbClusterDetailModel({
							...rdbClusterDetailModel,
							rdbId: record.id,
						})

						setRdbInstanceModel({
							...rdbInstanceModel,
							rdbId: record.id,
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
			<ProTable<API.Rdb>
				rowKey={(record) => String(record.id)}
				// 请求数据API
				columns={columns}
				// 分页设置,默认数据,不展示动态调整分页大小
				params={{
					projectId: rdbClusterMgtModel.projectId
				}}
				request={queryProjectRdb}
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
								setRdbClusterEditingModel({
									...rdbClusterEditingModel,
									showDrawer: true,
									projectId: rdbClusterMgtModel.projectId,
									from: 'create'
								})
							}}>
							新增
						</Button>
					</Button.Group>
				]}
			/>
			<RdbClusterEditing />
			<RdbClusterConfig />
		</div>
	)
};

export default RdbClusterMgt;
