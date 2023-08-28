import React, {useRef} from 'react';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable, {ActionType} from '@ant-design/pro-table';
import {Button} from 'antd';
import {EditOutlined, PlusOutlined} from '@ant-design/icons';
import {useModel} from 'umi';
import {pagedQueryDataSystemResource} from '@/services/a-cdc/api';
import StarRocksClusterEditing from '../StarRocksClusterEditing';
import StarRocksClusterConfig from '../StarRocksClusterConfig';
import {DataSystemResourceTypeConstant} from "@/services/a-cdc/constant/DataSystemResourceTypeConstant";

const StarRocksClusterMgt: React.FC = () => {
	// 数据流
	const {starRocksClusterEditingModel, setStarRocksClusterEditingModel} = useModel('StarRocksClusterEditingModel')
	const {starRocksClusterConfigModel, setStarRocksClusterConfigModel} = useModel('StarRocksClusterConfigModel')
	const {starRocksClusterMgtModel} = useModel('StarRocksClusterMgtModel')
	const {starRocksClusterDetailModel, setStarRocksClusterDetailModel} = useModel('StarRocksClusterDetailModel')
	const {starRocksInstanceModel, setStarRocksInstanceModel} = useModel('StarRocksInstanceModel')

  const ref = useRef<ActionType>();

	const columns: ProColumns<API.DataSystemResource>[] = [
    {
      title: '数据系统类型',
      width: "10%",
      dataIndex: 'dataSystemType',
      search: false,
    },

		{
			title: '名称',
			width: "30%",
			dataIndex: 'name',
		},

		{
			title: '描述',
			width: "40%",
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
						setStarRocksClusterEditingModel({
							...starRocksClusterEditingModel,
							showDrawer: true,
							projectId: starRocksClusterMgtModel.projectId,
							resourceId: record.id,
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
						setStarRocksClusterConfigModel({
							...starRocksClusterConfigModel,
							showDrawer: true,
              resourceId: record.id,
              dataSystemType: record.dataSystemType
						})

						setStarRocksClusterDetailModel({
							...starRocksClusterDetailModel,
              resourceId: record.id,
              description: record.description,
              dataSystemType: record.dataSystemType
						})

						setStarRocksInstanceModel({
							...starRocksInstanceModel,
              resourceId: record.id,
              dataSystemType: record.dataSystemType
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
			<ProTable<API.DataSystemResource>
				rowKey={(record) => String(record.id)}
        actionRef={ref}
				// 请求数据API
				columns={columns}
				// 分页设置,默认数据,不展示动态调整分页大小
				params={{
					projectIds: [starRocksClusterMgtModel.projectId],
          resourceTypes: [DataSystemResourceTypeConstant.STARROCKS_CLUSTER]
				}}
				request={pagedQueryDataSystemResource}
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
								setStarRocksClusterEditingModel({
									...starRocksClusterEditingModel,
									showDrawer: true,
									projectId: starRocksClusterMgtModel.projectId,
									from: 'create'
								})
							}}>
							新增
						</Button>
					</Button.Group>
				]}
			/>
			<StarRocksClusterEditing tableRef={ref.current} />
			<StarRocksClusterConfig />
		</div>
	)
};

export default StarRocksClusterMgt;
