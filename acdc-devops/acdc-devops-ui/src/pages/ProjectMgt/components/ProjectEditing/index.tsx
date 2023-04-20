import React, {useEffect, useRef, useState} from 'react';
import ProForm, {DrawerForm, ProFormInstance, ProFormSelect, ProFormText} from '@ant-design/pro-form';
import {useModel} from 'umi';
import {EditOutlined} from '@ant-design/icons';
import {message, Modal} from 'antd';
import {createProject, editProject, getProject, queryUser} from '@/services/a-cdc/api';
import {ActionType} from "@ant-design/pro-table";
const {confirm} = Modal;

type UserSelectItem = {
	value?: string;
	label?: string;
};

const ProjectEditor: React.FC<{tableRef: ActionType}> = ({tableRef}) => {
	// 项目添加抽屉
	const {projectEditingModel, setProjectEditingModel} = useModel('ProjectEditingModel')

	const [userSelectData, setUserSelectData] = useState<UserSelectItem[]>();

	const formRef = useRef<
		ProFormInstance<{
			name: string;
			description?: string;
		}>
	>();

	//	发生在首次挂载(mount), 更新state不会再次触发
	useEffect(() => {
		initUserSelectData('');
	}, [projectEditingModel.projectId]);

	const initUserSelectData = async (keyWords: string) => {
		// mock
		//return [
		//{value: '520000201604258831', label: 'Patricia Lopez'},
		//{value: '150000197210168659', label: 'Sandra Hall'},
		//]
		// 下拉数据填充
		let userSelectResult: API.AcdcUser[] = await queryUser({})
		let userSelectItems: UserSelectItem[] = [];
		userSelectResult!.forEach((record, index, arr) => {
			userSelectItems.push({
				value: record.email!,
				label: record.email!,
			})
		})
		setUserSelectData(userSelectItems)

	}

	const initFormValue = async () => {
		if (projectEditingModel.from == 'create') {
			return;
		}
		// 表单数据
		let project: API.Rdb = await getProject({projectId: projectEditingModel.projectId!})
		formRef?.current?.setFieldsValue({
			name: project.name,
			description: project.description,
			ownerEmail: project.ownerEmail
		});
	}

	const submitForm = async () => {
		// 这个校验,如果表单校验失败,await会阻塞住
		await formRef.current?.validateFields();
		confirm({
			title: '确定提交吗',
			icon: <EditOutlined />,
			content: '增加项目',
			async onOk() {
				let formObj = formRef.current?.getFieldsFormatValue?.();
				if ('edit' == projectEditingModel.from) {
					let editBody: API.Project = {
						id: projectEditingModel.projectId,
						name: formObj!.name,
						description: formObj!.description,
						ownerEmail: formObj!.ownerEmail,
					}
					await editProject(editBody)
          await tableRef.reload(false)
					message.info("修改成功")
				}
				else {
					let createBody: API.Project = {
						name: formObj!.name,
						description: formObj!.description,
						ownerEmail: formObj!.ownerEmail,
					}
					await createProject(createBody)
          await tableRef.reload(false)
					message.info("创建成功")
				}

				setProjectEditingModel({
					...projectEditingModel,
					showDrawer: false
				})
			},
			onCancel() {},
		});
	}

	return (
		<div>
			<DrawerForm<{
			}>
				title="项目信息"
				visible={projectEditingModel.showDrawer}
				width={"35%"}
				drawerProps={{
					forceRender: false,
					destroyOnClose: true,
					onClose: () => {
						setProjectEditingModel({
							...setProjectEditingModel,
							showDrawer: false
						})
					},
				}}
				onFinish={submitForm}
				onInit={() => {initFormValue()}}
			>

				<div>
					<ProForm<{
						name?: string;
						description?: string;
						ownerEmail?: string;
					}>
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
						<ProFormText
							width="md"
							name="name"
							label="项目名称"
							placeholder="请输入项目名称"
							rules={[{required: true, message: '项目名称必填'}]}

						/>
						<ProFormText
							width="md"
							name="description"
							label="项目描述"
							placeholder="请输入项目描述"
							rules={[{required: true, message: '项目描述必填'}]}
						/>

						<ProFormSelect
							name="ownerEmail"
							label="项目负责人"
							showSearch
							//debounceTime={300}
							request={
								async (keyWords) => {
									return userSelectData
								}
							}
							placeholder="请选择项目负责人"
							rules={[{required: true, message: '项目负责人必填'}]}
						/>

					</ProForm>
				</div>

			</DrawerForm>
		</div>
	)
};

export default ProjectEditor;
