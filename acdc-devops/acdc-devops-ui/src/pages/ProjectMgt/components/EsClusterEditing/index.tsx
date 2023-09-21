import React, {useRef} from 'react';
import ProForm, {ModalForm, ProFormInstance, ProFormText, ProFormTextArea} from '@ant-design/pro-form';
import {useModel} from 'umi';
import {EditOutlined} from '@ant-design/icons';
import {message, Modal} from 'antd';
import {
  createDataSystemResource,
  getDataSystemResource,
  updateDataSystemResource
} from '@/services/a-cdc/api';
import {ActionType} from "@ant-design/pro-table";
import { DataSystemResourceTypeConstant } from '@/services/a-cdc/constant/DataSystemResourceTypeConstant';
import { DataSystemTypeConstant } from '@/services/a-cdc/constant/DataSystemTypeConstant';
const {confirm} = Modal;

const EsClusterEditing: React.FC<{tableRef: ActionType}> = ({tableRef}) => {
	// 数据流
	const {esClusterEditingModel, setEsClusterEditingModel} = useModel('EsClusterEditingModel')
	// form ref
	const formRef = useRef<
		ProFormInstance<{
			name: string;
			description?: string;
      nodeServers: string;
			username: string;
			password: string;
			clusterType: string;
		}>
	>();

	// 表单提交 创建|修改
	const submitForm = async () => {
		await formRef.current?.validateFields();
		confirm({
			title: '确定提交吗?',
			icon: <EditOutlined />,
			content: '增加/编辑集群',
      onOk: async function () {
        let formObj = formRef.current?.getFieldsFormatValue?.();

        // build data system resource
        const project: API.Project = {
          id: esClusterEditingModel.projectId
        }

        // configurations
        const bootstrapConfiguration: API.DataSystemResourceConfiguration = {
          name: "node.servers",
          value: formObj!.nodeServers
        }

        let esCluster: API.DataSystemResource = {
          name: formObj?.name!,
          description: formObj?.description,
          projects: [project],
          dataSystemType: DataSystemTypeConstant.ELASTICSEARCH,
          resourceType: DataSystemResourceTypeConstant.ELASTICSEARCH_CLUSTER,
          dataSystemResourceConfigurations: {
            "node.servers": bootstrapConfiguration,
          }
        }

        const usernameConfiguration: API.DataSystemResourceConfiguration = {
          name: "username",
          value: formObj!.username
        }
        const passwordConfiguration: API.DataSystemResourceConfiguration = {
          name: "password",
          value: formObj!.password
        }
        esCluster.dataSystemResourceConfigurations["username"] = usernameConfiguration;
        esCluster.dataSystemResourceConfigurations["password"] = passwordConfiguration;

        if ('edit' == esClusterEditingModel.from) {
          esCluster.id = esClusterEditingModel.esClusterId
          await updateDataSystemResource(esCluster)
        } else {
          console.log("will create ", esCluster)
          await createDataSystemResource(esCluster)
        }
        tableRef.reload()
        message.info("操作成功")
        setEsClusterEditingModel({
          ...esClusterEditingModel,
          showModal: false
        })
      },
			onCancel() {},
		});
	}

	// 初始化表单数据
	const initFormValue = async () => {
		if (esClusterEditingModel.from == 'create') {
			return;
		}
		// 表单数据
		let esCluster: API.DataSystemResource = await getDataSystemResource({id: esClusterEditingModel.esClusterId!})

		formRef?.current?.setFieldsValue({
			name: esCluster.name,
      nodeServers: esCluster.dataSystemResourceConfigurations["node.servers"].value,
			description: esCluster.description,
      username: esCluster.dataSystemResourceConfigurations["username"] ? esCluster.dataSystemResourceConfigurations["username"].value : ""
		});
	}

	return (
		<ModalForm
			title="集群信息"
			visible={esClusterEditingModel.showModal}
			modalProps={{
				destroyOnClose: true,
				onCancel: () => {
					setEsClusterEditingModel({
						...esClusterEditingModel,
						showModal: false
					})
				}
			}}
			onFinish={submitForm}
			onInit={() => {initFormValue()}}
		>
			<ProForm<{
				name?: string;
				username?: string;
				password?: string;
			}>
				formRef={formRef}
				formKey="esClusterEditingForm"
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

				initialValues={{
					clusterType: 'USER',
					version: '2.6.0',
				}}
			>
				<ProForm.Group>
					<ProFormText
						width="md"
						name="name"
						label="集群名称"
						placeholder="请输入集群名称"
						rules={[{required: true, message: '集群名称必填'}]}
					/>
					<ProFormText
						width="md"
						name="nodeServers"
						label="集群地址"
						placeholder="请输入集群地址"
						rules={[{required: true, message: '集群地址必填'}]}
					/>

				</ProForm.Group>

				<ProForm.Group>

					<ProFormText
						width="md"
						name="username"
						label="username"
						placeholder="username"
            rules={[{required: true, message: '用户名必填'}]}
					/>

					<ProFormText
						width="md"
						name="password"
						label="password"
						placeholder="password"
            rules={[{required: true, message: '密码必填'}]}
					/>

				</ProForm.Group>

				<ProFormTextArea
					name="description"
					label="集群描述"
					placeholder="请输入集群描述信息"
					rules={[
						{max: 50, message: '限制50个字符'}
					]
					}
				/>
			</ProForm>
		</ModalForm>
	)
};

export default EsClusterEditing;
