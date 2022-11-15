import React from 'react';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import {Button} from 'antd';
import {EditOutlined, PlusOutlined} from '@ant-design/icons';
import {useModel} from 'umi';
import {queryProject} from '@/services/a-cdc/api';
import ProjectConfig from './components/ProjectConfig';
import ProjectEditing from './components/ProjectEditing';

const ProjectList: React.FC = () => {
	// 编辑页,数据流
	const {projectEditingModel, setProjectEditingModel} = useModel('ProjectEditingModel')

	// 配置页数据流
	const {projectConfigModel, setProjectConfigModel} = useModel('ProjectConfigModel')

	// 用户页数据流
	const {projectUserModel, setProjectUserModel} = useModel('ProjectUserModel')

	// 项目信息页数据流
	const {projectDetailModel, setProjectDetailModel} = useModel('ProjectDetailModel')

	// RDB管理页数据流
	const {rdbClusterMgtModel, setRdbClusterMgtModel} = useModel('RdbClusterMgtModel')

	// Kafka集群管理页
	const {kafkaClusterMgtModel, setKafkaClusterMgtModel} = useModel('KafkaClusterMgtModel')

	const projectColumns: ProColumns<API.Project>[] = [
		{
			title: '名称',
			width: "25%",
			dataIndex: 'name',
		},
		{
			title: '项目负责人',
			width: "25%",
			dataIndex: 'ownerEmail',
			search: false,
		},

		{
			title: '描述',
			width: "25%",
			dataIndex: 'description',
			search:false,
			ellipsis: true,
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
						setProjectEditingModel({
							...projectEditingModel,
							showDrawer: true,
							projectId: record.id,
							from: 'edit'
						})
					}}
				>
					<EditOutlined />
					编辑
				</a>
				,
				<a
					key={'config'+record.id}
					onClick={() => {
						setProjectUserModel({
							...projectUserModel,
							projectId: record.id
						})
						setProjectConfigModel({
							...projectConfigModel,
							showDrawer: true
						})
						setProjectDetailModel({
							...projectDetailModel,
							projectId: record.id
						}
						)
						setRdbClusterMgtModel({
							...rdbClusterMgtModel,
							projectId: record.id,
						})

						setKafkaClusterMgtModel({
							...kafkaClusterMgtModel,
							projectId: record.id
						})
					}}
				>
					<EditOutlined />
					配置
				</a>
			],
		},
	];
	return (
		<div>
			<ProTable<API.Project>
				rowKey={(record) => String(record.id)}
				// 请求数据API
				columns={projectColumns}
				// 分页设置,默认数据,不展示动态调整分页大小
				params={{}}
				request={queryProject}
				pagination={{
					showSizeChanger: false,
					pageSize: 8
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
								setProjectEditingModel({
									...projectEditingModel,
									showDrawer: true,
									from: 'create'
								})
							}}>
							新增
						</Button>
					</Button.Group>
				]}
			/>

			<ProjectEditing/>
			<ProjectConfig/>
		</div>
	)
};

export default ProjectList;
