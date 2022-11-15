import React, {useState} from 'react';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import {getConnectorConfig, getSinkHiveLinkDetail, getSinkConnectorInfo, getSourceConnectorInfo, editConnector} from '@/services/a-cdc/api';
import {message, Drawer, Modal} from 'antd';
import {queryConnectors, editConnectorStatus} from '@/services/a-cdc/api';
import {useModel} from 'umi';
import {DrawerForm, } from '@ant-design/pro-form';
import {EditOutlined, PoweroffOutlined, PlaySquareOutlined} from '@ant-design/icons';
import ConnectorDetail from './components/ConnectorDetail';
import FieldMappingList from '../components/FieldMapping';
import {handleFieldMappingItem, verifyPrimaryKey} from '@/services/a-cdc/connector/field-mapping';
const {confirm} = Modal;

const ConnectorList: React.FC = () => {
	// 全局数据流
	const {connectorModel, setConnectorModel} = useModel('ConnectorModel')

	const {fieldMappingModel, setFieldMappingModel} = useModel('FieldMappingModel')

	// 详情抽屉控制
	//const [showDetail, setShowDetail] = useState<boolean>(false);

	// 编辑抽屉控制
	//const [showEdit, setShowEdit] = useState<boolean>(false);

	// 模糊搜索
	const [searchConnectorName, setSearchConnectorName] = useState<string>();

	/**
		提交修改表单
	*/
	const submitFieldMappingEdit = async () => {
		if (!connectorModel.connectorId) {
			return false
		}

		if (!fieldMappingModel || !fieldMappingModel.fieldMappings) {
			message.error("错误的字段映射");
			return false;
		}
		let fieldMappings: API.FieldMappingListItem[] = fieldMappingModel!.fieldMappings!;

		// 主键字段校验
		let sinkDataSystemType = connectorModel!.sinkConnectorInfo!.sinkDataSystemType
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

			// TODO kafka

			return false;
		}

		confirm({
			title: '确定提交吗',
			icon: <EditOutlined />,
			content: '修改字段映射',
			async onOk() {
				// 处理条件过滤
				let newItems = handleFieldMappingItem(fieldMappingModel?.fieldMappings!);
				let reqBody: API.ConnectorEditInfo = {
					connectorId: connectorModel?.connectorId,
					fieldMappings: newItems
				}
				await editConnector({...reqBody});
				message.success('修改成功');
				setConnectorModel({
					...connectorModel,
					refreshVersion: connectorModel.refreshVersion! + 1,
					showEdit: false
				})
				//setShowEdit(false)
			},
			onCancel() {},
		});
	}

	/**
	 更新connector 状态
	*/
	const editStatus = (connectorId?: number, connectorName?: string, state?: string) => {
		confirm({
			title: '确定操作吗',
			icon: <EditOutlined />,
			content: '修改任务状态',
			async onOk() {
				// 处理条件过滤
				await editConnectorStatus({
					connectorId: connectorId,
					state: state
				});
				message.success('操作成功');
				setConnectorModel({
					...connectorModel,
					refreshVersion: connectorModel.refreshVersion! + 1
				})
			},
			onCancel() {},
		});
	}

	const fetchFieldMapping = async (item: API.ConnectorListItem) => {
		if (item.connectorType != 'sink') {
			return
		}

		let sinkInfo = await getSinkConnectorInfo({id: item.id})
		setFieldMappingModel({
			connectorId: item.id,
			from: 'edit',
			srcDataSetId: sinkInfo!.srcDataSetId!,
			sinkDataSystemType: sinkInfo!.sinkDataSystemType,
		})
		setConnectorModel({
			...connectorModel,
			connectorId: item.id,
			connectorType: item.connectorType,
			sinkConnectorInfo: sinkInfo,
			showEdit: true,
		})

		//setShowEdit(true)
	}


	/**
	 获取链路详情
	*/
	const fetchConnectorInfo = async (item: API.ConnectorListItem) => {

		let config = await getConnectorConfig({id: item.id})

		if (item.connectorType == 'SOURCE') {
			let sourceInfo = await getSourceConnectorInfo({id: item.id})
			setConnectorModel({
				...connectorModel,
				connectorId: item.id,
				connectorType: item.connectorType,
				sourceConnectorInfo: sourceInfo,
				connectorConfig: config,
				showDetail: true,
			})
			setFieldMappingModel({
				connectorId: item.id,
				from: 'detail',
				sinkDataSystemType: '',
				srcDataSystemType: sourceInfo!.srcDataSystemType
			})

		} else {
			let sinkInfo = await getSinkConnectorInfo({id: item.id})
			setConnectorModel({
				...connectorModel,
				connectorId: item.id,
				connectorType: item.connectorType,
				sinkConnectorInfo: sinkInfo,
				connectorConfig: config,
				showDetail: true,
			})
			setFieldMappingModel({
				connectorId: item.id,
				from: 'detail',
				srcDataSystemType: '',
				sinkDataSystemType: sinkInfo!.sinkDataSystemType
			})

		}
	}

	const connectorColumns: ProColumns<API.ConnectorListItem>[] = [
		// {
		// 	title: 'ID',
		// 	dataIndex: 'id',
		// },
		{
			title: '名称',
			width: "46%",
			dataIndex: 'name',
			render: (dom, entity) => {
				return (
					<a
						onClick={() => {
							//setShowDetail(true)
							fetchConnectorInfo(entity)
						}}
					>
						{dom}
					</a>
				);
			},
		},
		{
			title: '创建时间',
			width: "15%",
			dataIndex: 'creationTimeFormat'
		},
		{
			width: "15%",
			title: '修改时间',
			dataIndex: 'updateTimeFormat'
		},
		{
			title: '预期状态',
			width: "6%",
			dataIndex: 'desiredState',
			//render: (_, record) => (
			//<Space>
			//<Tag color='green'>
			//初始化
			//</Tag>
			//</Space>
			//),
		},
		{
			title: '实际状态',
			width: "6%",
			dataIndex: 'actualState',

			//render: (_, record) => (
			//<Space>
			//<Tag color='green'>
			//初始化
			//</Tag>
			//</Space>
			//),
		},
		{
			title: '操作',
			width: "12%",
			valueType: 'option',
			dataIndex: 'option',
			render: (text, record, _, action) => [
				<>
					{
						record.desiredState == 'RUNNING' ?
							<a onClick={() => {editStatus(record.id, record.name, 'STOPPED')}}>
								<PlaySquareOutlined />
								停止
							</a>
							:
							<a onClick={() => {editStatus(record.id, record.name, 'RUNNING')}}>
								<PoweroffOutlined />
								启动
							</a>
					}
				</>
			],
		},
	];

	return (
		<div>
			<ProTable<API.ConnectorListItem, API.ConnectorQuery>
				params={{
					name: searchConnectorName,
					refreshVersion: connectorModel!.refreshVersion!
				}}
				rowKey="id"
				// 请求数据API
				request={ queryConnectors }
				columns={connectorColumns}
				toolbar={{
					search: {
						onSearch: (value: string) => {
							setSearchConnectorName(value);
						},
					},
				}}
				// 分页设置,默认数据,不展示动态调整分页大小
				pagination={{
					showSizeChanger: false,
					pageSize: 8
				}}
				options={false}
				search={false}
			/>

			{/*
				父组件刷新,会导致子组件刷新,所以父组件如果使用state变量,则可以通过变量
				传递给子组件,让子组件更新
				*/
			}
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

			<DrawerForm<{
				name: string;
				company: string;
			}>
				title="修改字映射"
				visible={connectorModel.showEdit}
				width={"90%"}
				drawerProps={{
					forceRender: false,
					destroyOnClose: true,
					onClose: () => {
						setConnectorModel({
							...connectorModel,
							showEdit: false,
						})
					}
				}}
				onFinish={submitFieldMappingEdit}

			>
				<FieldMappingList />
			</DrawerForm>

		</div>
	)
};

export default ConnectorList;
