import React, {useState} from 'react';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import {message, Drawer, Modal} from 'antd';
import {useModel} from 'umi';
import {DrawerForm, } from '@ant-design/pro-form';
import {EditOutlined, PoweroffOutlined, PlaySquareOutlined} from '@ant-design/icons';
import {editRdbMysql, getRdbMysql, queryRdb, editConnector} from '@/services/a-cdc/api';
import RdbMysqlEdit from './components/EidtPage';
const {confirm} = Modal;

const ConnectorList: React.FC = () => {
	// 全局数据流
	const {rdbMysqlModel,setRdbMysqlModel} = useModel('RdbMysqlModel')

	// 编辑抽屉控制
	const [showEdit, setShowEdit] = useState<boolean>(false);

	// 模糊搜索
	const [searchRdbName, setSearchRdbName] = useState<string>();

	let tmpHost: string | undefined = ''
	let tmpPort: string | undefined = ''

	/**
		提交修改表单
	*/
	const submitEdit = async () => {
		//setShowEdit(false)
		confirm({
			title: '确定提交吗',
			icon: <EditOutlined />,
			content: '修改实例配置',
			async onOk() {
				// 处理条件过滤
				let reqBody = {
					rdbId: rdbMysqlModel?.rdbId,
					host: tmpHost,
					port: tmpPort,
				}

				if ('' === reqBody.port || '' === reqBody.host || !reqBody.rdbId) {
					message.warn("请输入表单信息")
					return;
				}

				await editRdbMysql({...reqBody});
				message.success('修改成功');
				setRdbMysqlModel({
					...rdbMysqlModel,
					refreshVersion: rdbMysqlModel.refreshVersion! + 1
				})
				setShowEdit(false)
			},
			onCancel() {},
		});
	}

	const fetchRdbMysql = async (rdbId: number) => {
		let result = await getRdbMysql({rdbId: rdbId});
		setRdbMysqlModel({
			...rdbMysqlModel,
			source:'detail',
			rdbId: result.rdbId,
			host: result.host,
			port: result.port
		})
	}

	const rdbColumns: ProColumns<API.RdbListItem>[] = [
		{
			title: '名称',
			width: "20%",
			dataIndex: 'name',
			render: (dom, entity) => {
				return (
					<a
						key="editable"
						onClick={()=>{
							setShowEdit(true)
							fetchRdbMysql(entity.id)
						}}
				>
				{dom}
				</a>
				);
			},
		},
		{
			title: '类型',
			width: "15%",
			dataIndex: 'rdbType'
		},
		{
			width: "15%",
			title: '描述',
			dataIndex: 'desc'
		},
		{
			width: "15%",
			title: '创建时间',
			dataIndex: 'creationTime'
		},

		{
			title: '修改时间',
			width: "15%",
			dataIndex: 'updateTime',
		},
		{
			title: '操作',
			width: "10%",
			valueType: 'option',
			render: (text, record, _, action) => [
				<a
					key="editable"
					onClick={() => {
						setRdbMysqlModel({
							...rdbMysqlModel,
							source: 'edit',
							rdbId: record.id,
							rdbType: record.rdbType,
							host:undefined,
							port:undefined,
						})
						setShowEdit(true)
					}}
				>
					<EditOutlined />
					编辑
				</a>
			],
		},
	];

	return (
		<div>
			<ProTable<API.RdbListItem, API.RdbQuery>
				params={{
					rdbType:'mysql',
					name: searchRdbName,
				}}

				// 请求数据API
				request={queryRdb}
				columns={rdbColumns}
				toolbar={{
					search: {
						onSearch: (value: string) => {
							setSearchRdbName(value);
						},
					},
				}}

				// 分页设置,默认数据,不展示动态调整分页大小
				pagination={{
					showSizeChanger: false,
					pageSize: 8
				}}
				options={false}
				rowKey="id"
				search={false}
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
				title="配置实例"
				visible={showEdit}
				width={"35%"}
				drawerProps={{
					forceRender: false,
					destroyOnClose: true,
					onClose:()=>{setShowEdit(false)}
				}}
				onFinish={submitEdit}
			>
				<RdbMysqlEdit 
					onHostChange={
						(_host) => {
							tmpHost = _host;
						}
					}
					onPortChange={
						(_port) => {
							tmpPort = _port
						}
					} />
			</DrawerForm>

		</div>
	)
};

export default ConnectorList;

