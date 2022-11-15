import React, {useState} from 'react';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import {deleteConnection, editConnection, editConnectionStatus, getConnectionActualStatus, getConnectionDetail, getConnectorConfig, getSinkConnectorInfo, getSourceConnectorInfo, queryConnection} from '@/services/a-cdc/api';
import {message, Drawer, Modal} from 'antd';
import {useAccess, useModel} from 'umi';
import {DrawerForm, } from '@ant-design/pro-form';
import {EditOutlined, PoweroffOutlined, PlaySquareOutlined, CopyOutlined} from '@ant-design/icons';
import ConnectionFieldMappingList from '../components/FieldMapping';
import {handleFieldMappingItem, verifyPrimaryKey} from '@/services/a-cdc/connection/field-mapping';
import ConnectionInfo from '../ConnectionInfo';
import ConnectorDetail from '@/pages/connector/ConnectorMgt/components/ConnectorDetail';
const {confirm} = Modal;

const ConnectionList: React.FC = () => {
	// 全局数据流
	const {connectionModel, setConnectionModel} = useModel('ConnectionModel')
	// 全局数据流
	const {connectorModel, setConnectorModel} = useModel('ConnectorModel')
	const {connectionFieldMappingModel, setConnectionFieldMappingModel} = useModel('ConnectionFieldMappingModel')
	const {setFieldMappingModel} = useModel('FieldMappingModel')


	const [connectionDetail, setConnectionDetail] = useState<API.ConnectionDetail>({})

	const [showDetail, setShowDetail] = useState<boolean>(false);

	const access = useAccess();


	/**
		提交修改表单
	*/
	const submitFieldMappingEdit = async () => {
		if (!connectionModel.connectionId) {
			return false
		}

		if (!connectionFieldMappingModel || !connectionFieldMappingModel.fieldMappings) {
			message.error("错误的字段映射");
			return false;
		}
		let fieldMappings: API.FieldMappingListItem[] = connectionFieldMappingModel!.fieldMappings!;

		// 主键字段校验
		let sinkDataSystemType = connectionModel!.sinkDataSystemType
		if (!verifyPrimaryKey(fieldMappings, sinkDataSystemType!)) {
			if (sinkDataSystemType == 'HIVE') {
				message.warn("源表不存在主键字段")
			}

			if (
				sinkDataSystemType == 'MYSQL'
				|| sinkDataSystemType == 'TIDB'
			) {
				message.warn("源表主键与目标表主键类型不一致")
			}

			return false;
		}

		confirm({
			title: '确定提交吗',
			icon: <EditOutlined />,
			content: '修改字段映射',
			async onOk() {
				// 处理条件过滤
				let newItems = handleFieldMappingItem(connectionFieldMappingModel?.fieldMappings!);
				let reqBody: API.ConnectionEditInfo = {
					connectionId: connectionModel?.connectionId!,
					fieldMappings: newItems
				}
				await editConnection([{...reqBody}]);
				message.success('修改成功');
				setConnectionModel({
					...connectionModel,
					refreshVersion: connectionModel.refreshVersion! + 1,
					showEdit: false
				})
			},
			onCancel() {},
		});
	}

	/**
 获取链路详情
*/
	const fetchSinkConnectorInfo = async (item: API.Connection) => {

		let config = await getConnectorConfig({id: item.sinkConnectorId})
		let sinkInfo = await getSinkConnectorInfo({id: item.sinkConnectorId})
		setConnectorModel({
			...connectorModel,
			connectorId: item.sinkConnectorId,
			connectorType: 'SINK',
			sinkConnectorInfo: sinkInfo,
			connectorConfig: config,
			showDetail: true,
		})
		setFieldMappingModel({
			connectorId: item.sinkConnectorId,
			from: 'detail',
			srcDataSystemType: '',
			sinkDataSystemType: 'mysql'
		})
	}

	const fetchSourceConnectorInfo = async (item: API.Connection) => {
		let config = await getConnectorConfig({id: item.sourceConnectorId})
		let sourceInfo = await getSourceConnectorInfo({id: item.sourceConnectorId})
		setConnectorModel({
			...connectorModel,
			connectorId: item.sinkConnectorId,
			connectorType: 'SOURCE',
			sourceConnectorInfo: sourceInfo,
			connectorConfig: config,
			showDetail: true,
		})
	}

	/**
	 更新connection 状态
	*/
	const editStatus = (connectionId?: number, state?: string, requisitionState?: string) => {
		if ('APPROVED' != requisitionState) {
			message.warn("您申请的链路还未审批通过,不能启停链路")
			return;
		}

		confirm({
			title: '确定操作吗',
			icon: <EditOutlined />,
			content: '修改链路状态',
			async onOk() {
				// 处理条件过滤
				await editConnectionStatus({
					connectionId: connectionId,
					state: state
				});
				message.success('操作成功');
				setConnectionModel({
					...connectionModel,
					refreshVersion: connectionModel.refreshVersion! + 1
				})
			},
			onCancel() {},
		});
	}

	/**
	 删除 connection
	*/
	const delConnection = async (connectionId?: number) => {
		// 必须停止链路才可以修改,可能会触发审批
		let status = await getConnectionActualStatus({id: connectionId})

		if (status != 'STOPPED') {
			message.warn('请停止链路');
			return;
		}

		confirm({
			title: '确定删除吗?',
			icon: <EditOutlined />,
			content: '删除链路',
			async onOk() {
				// 处理条件过滤
				await deleteConnection({
					connectionId: connectionId,
				});
				message.success('操作成功');
				setConnectionModel({
					...connectionModel,
					refreshVersion: connectionModel.refreshVersion! + 1
				})
			},
			onCancel() {},
		});
	}


	const fetchConnectionDetail = async (id: number) => {
		let detail = await getConnectionDetail(id)
		setConnectionDetail(detail!)
		setShowDetail(true)
	}

	const fetchFieldMapping = async (item: API.Connection) => {
		// 必须停止链路才可以修改,可能会触发审批
		let status = await getConnectionActualStatus({id: item.connectionId})

		if ('APPROVED' != item.requisitionState) {
			message.warn('您申请的链路还未审批通过,不能编辑配置');
			return;
		}
		if (status != 'STOPPED') {
			message.warn('请停止链路');
			return;
		}

		setConnectionFieldMappingModel({
			connectionId: item.connectionId,
			from: 'edit',
			srcDataSetId: item.sourceDatasetId!,
			sinkDataSetId: item.sinkDatasetId!,
			sinkDataSystemType: item.sinkDataSystemType,
		})
		setConnectionModel({
			...connectionModel,
			connectionId: item.connectionId,
			sinkDataSystemType: item.sinkDataSystemType,
			showEdit: true,
		})
	}

	const connectionColumns: ProColumns<API.Connection>[] = [
		{
			title: '类型',
			width: "10%",
			dataIndex: 'sinkDataSystemType',
			ellipsis: true,
			onFilter: true,
			valueType: 'select',
			formItemProps: {
				rules: [
					{
						required: true,
						message: '此项为必填项',
					},
				],
			},
			initialValue: 'HIVE',
			valueEnum: {
				MYSQL: {text: 'MYSQL', status: 'Success'},
				TIDB: {text: 'TIDB', status: 'Success'},
				HIVE: {text: 'HIVE', status: 'Success'},
				KAFKA: {text: 'KAFKA', status: 'Success'},
			},

		},

		{
			title: '申请状态 ',
			width: "7%",
			dataIndex: 'requisitionState',
			valueType: 'select',
			valueEnum: {
				APPROVING: {text: '审批中', status: 'APPROVING'},
				REFUSED: {text: '审批拒绝', status: 'REFUSED'},
				APPROVED: {text: '审批通过', status: 'APPROVED'},
			},

		},
		{
			title: '运行状态',
			width: "7%",
			dataIndex: 'actualState',
			valueType: 'select',
			valueEnum: {
				STARTING: {text: '启动中', status: 'STARTING'},
				RUNNING: {text: '运行中', status: 'RUNNING'},
				STOPPING: {text: '停止中', status: 'STOPPING'},
				STOPPED: {text: '已停止', status: 'STOPPED'},
				FAILED: {text: '运行失败', status: 'FAILED'},
			},
		},

		{
			title: '集群',
			width: "12%",
			dataIndex: 'sinkDatasetClusterName',
			ellipsis: true,
			copyable: true,
		},

		{
			title: '数据库',
			width: "10%",
			dataIndex: 'sinkDatasetDatabaseName',
			ellipsis: true,
			copyable: true,
		},

		{
			title: '数据集',
			width: "20%",
			dataIndex: 'sinkDatasetName',
			ellipsis: true,
			copyable: true,
		},
		{
			title: '创建时间',
			width: "16%",
			dataIndex: 'creationTimeFormat',
			search: false,
		},
		{
			title: '操作',
			width: "32%",
			valueType: 'option',
			dataIndex: 'option',
			render: (text, record, _, action) => [
				<a
					key={"info_" + record.connectionId}
					onClick={() => {
						fetchConnectionDetail(record.connectionId!)
					}}
				>
					<CopyOutlined />
					详情
				</a>
				,
				<a
					key={"editable_" + record.connectionId}
					onClick={() => {
						fetchFieldMapping(record)
					}}
				>
					<EditOutlined />
					编辑
				</a>
				,
				<>
					{
						record.desiredState == 'RUNNING' ?
							<a key={"stop_" + record.connectionId} onClick={() => {editStatus(record.connectionId, 'STOPPED', record.requisitionState)}}>
								<PlaySquareOutlined />
								停止
							</a>
							:
							<a key={"start_" + record.connectionId} onClick={() => {editStatus(record.connectionId, 'RUNNING', record.requisitionState)}}>
								<PoweroffOutlined />
								启动
							</a>
					}
				</>
				,

				<div>
					{
						access.canAdmin ?

							<a
								key={'delete_' + record.connectionId}
								onClick={() => {
									delConnection(record.connectionId)
								}}
							>
								删除
							</a>
							: <></>
					}
				</div>
				,
				<div>
					{
						access.canAdmin ?
							<a
								key={'sink_task_' + record.connectionId}
								onClick={() => {
									fetchSinkConnectorInfo(record)
								}}
							>
								sink
							</a>
							: <></>
					}
				</div>
				,
				<div>
					{
						access.canAdmin ?
							<a
								key={'source_task_' + record.connectionId}
								onClick={() => {
									fetchSourceConnectorInfo(record)
								}}
							>
								source
							</a>
							: <></>
					}
				</div>
			],
		},
	];

	return (
		<div>
			<ProTable<API.Connection>
				params={{
					refreshVersion: connectionModel!.refreshVersion!
				}}
				rowKey={(record) => String(record.connectionId)}
				// 请求数据API
				request={queryConnection}
				columns={connectionColumns}
				toolbar={{
				}}
				// 分页设置,默认数据,不展示动态调整分页大小
				pagination={{
					showSizeChanger: true,
					pageSize: 10
				}}
				options={{
					setting: {
						listsHeight: 400,
					},
				}}

				search={{collapsed: false}}

				form={{
					ignoreRules: false,
				}}

			/>

			{/*
				父组件刷新,会导致子组件刷新,所以父组件如果使用state变量,则可以通过变量
				传递给子组件,让子组件更新
				*/
			}
			<DrawerForm<{
				name: string;
				company: string;
			}>
				title="修改字映射"
				visible={connectionModel.showEdit}
				width={"90%"}
				drawerProps={{
					forceRender: false,
					destroyOnClose: true,
					onClose: () => {
						setConnectionModel({
							...connectionModel,
							showEdit: false,
						})
					}
				}}
				onFinish={submitFieldMappingEdit}

			>
				<ConnectionFieldMappingList />
			</DrawerForm>

			<Drawer
				width={"100%"}
				visible={showDetail}
				onClose={() => {
					setShowDetail(false);
				}}
				closable={true}
			>
				<ConnectionInfo connectionDetail={connectionDetail} />
			</Drawer>

			<Drawer
				width={"100%"}
				visible={connectorModel.showDetail}
				onClose={() => {
					//setShowDetail(false);
					setConnectorModel({
						...connectorModel,
						showDetail: false,
					})
				}}
				closable={true}
			>
				<ConnectorDetail />
			</Drawer>

		</div>
	)
};

export default ConnectionList;
