import React, {useEffect, useRef, useState} from 'react';
import ProForm, {DrawerForm, ProFormInstance, ProFormSelect, ProFormText} from '@ant-design/pro-form';
import {useModel} from 'umi';
import {EditOutlined} from '@ant-design/icons';
import {message, Modal} from 'antd';
import {createRdb, editRdb, getRdb} from '@/services/a-cdc/api';
const {confirm} = Modal;

const RdbClusterEditing: React.FC = () => {
	// 数据流
	const {rdbClusterEditingModel, setRdbClusterEditingModel} = useModel('RdbClusterEditingModel')
	// form ref
	const formRef = useRef<
		ProFormInstance<{
			name: string;
			description?: string;
		}>
	>();

//	发生在首次挂载(mount), 更新state不会再次触发
	useEffect(() => {
	}, [rdbClusterEditingModel.rdbId]);

	const initFormValue = async () => {
		if (rdbClusterEditingModel.from == 'create') {
			return;
		}
		// 表单数据
		let rdb: API.Rdb = await getRdb({rdbId: rdbClusterEditingModel.rdbId!})
		formRef?.current?.setFieldsValue({
			name: rdb!.name,
			desc: rdb!.desc,
			rdbType: rdb!.rdbType,
			username: rdb!.username,
		});
	}


	const submitForm = async () => {
		await formRef.current?.validateFields();
		confirm({
			title: '确定提交吗?',
			icon: <EditOutlined />,
			content: '增加集群',
			async onOk() {
				let formObj = formRef.current?.getFieldsFormatValue?.()
				if ('create' == rdbClusterEditingModel.from) {
					let createBody: API.Rdb = {
						name: formObj!.name,
						rdbType: formObj!.rdbType,
						username: formObj!.username,
						password: formObj!.password,
						desc: formObj!.desc,
						projectId: rdbClusterEditingModel.projectId,
					}
					createRdb(createBody)
					message.info("添加成功")

				} else {
					let editBody: API.Rdb = {
						id: rdbClusterEditingModel.rdbId,
						name: formObj!.name,
						rdbType: formObj!.rdbType,
						username: formObj!.username,
						password: formObj!.password,
						desc: formObj!.desc,
						projectId: rdbClusterEditingModel.projectId,
					}
					editRdb(editBody)
					message.info("修改成功")
				}

				setRdbClusterEditingModel({
					...rdbClusterEditingModel,
					showDrawer: false
				})
			},
			onCancel() {},
		});
	}

	return (
		<div>
			<DrawerForm<{}>
				title="集群信息"
				visible={rdbClusterEditingModel.showDrawer}
				width={"35%"}
				style={{position: 'absolute'}}
				drawerProps={{
					forceRender: false,
					destroyOnClose: true,
					mask:true,
					autoFocus:true,
					placement:'right',
					onClose: () => {
						setRdbClusterEditingModel({
							...rdbClusterEditingModel,
							showDrawer: false
						})
					}
				}}
				onFinish={submitForm}
				onInit={() => {initFormValue()}}
			>

				<ProForm<{}>
					formRef={formRef}
					formKey="projectCreationForm"
					autoFocusFirstInput
					submitter={{
						// 配置按钮文本
						searchConfig: {
							resetText: '重置',
							submitText: '提交',
						},
						// 配置按钮的属性
						resetButtonProps: {
							style: {
								// 隐藏重置按钮
								display: 'none',
							},
						},
						// 隐藏提交按钮
						submitButtonProps: {},
						// 完全自定义整个区域
						render: (props, doms) => {
							return [];
						},
					}}

				>
					<ProFormSelect
						name="rdbType"
						label="集群类型"
						width="md"
						valueEnum={{
							tidb: 'TIDB',
							mysql: 'MYSQL',
						}}
						placeholder="请选择集群类型,tidb/mysql"
						rules={[{required: true, message: '集群类型必填'}]}
					/>
					<ProFormText
						width="md"
						name="name"
						label="集群名称"
						placeholder="请输入集群名称"
						rules={[{required: true, message: '集群名称必填'}]}
					/>
					<ProFormText
						width="md"
						name="username"
						label="用户名"
						placeholder="请输入用户名"
						rules={[{required: true, message: '用户名必填'}]}
					/>

					<ProFormText
						width="md"
						name="password"
						label="密码"
						placeholder="请输入密码"
						rules={[{required: true, message: '用户名必填'}]}
					/>

					<ProFormText
						width="md"
						name="desc"
						label="集群描述"
						placeholder="请输入集群描述信息"
						rules={[{required: true, message: '集群描述信息必填'}]}
					/>
				</ProForm>

			</DrawerForm>
		</div>
	)
};

export default RdbClusterEditing;
