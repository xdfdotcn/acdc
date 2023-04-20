import React, {useEffect, useRef, useState} from 'react';
import ProForm, {ModalForm, ProFormInstance, ProFormSelect, ProFormText, ProFormTextArea} from '@ant-design/pro-form';
import {useModel} from 'umi';
import {EditOutlined} from '@ant-design/icons';
import {message, Modal} from 'antd';
import {
  createDataSystemResource,
  getDataSystemResource,
  updateDataSystemResource
} from '@/services/a-cdc/api';
import {ActionType} from "@ant-design/pro-table";
const {confirm} = Modal;

const KafkaClusterEditing: React.FC<{tableRef: ActionType}> = ({tableRef}) => {
	// 数据流
	const {kafkaClusterEditingModel, setKafkaClusterEditingModel} = useModel('KafkaClusterEditingModel')
	// form ref
	const formRef = useRef<
		ProFormInstance<{
			name: string;
			description?: string;
			bootstrapServers: string;
			securityProtocol: string;
			saslMechanism: string;
			saslUsername: string;
			saslPassword: string;
			clusterType: string;
		}>
	>();

	// kafka 安全协议动态disable

	const [saslDisabled, setSaslDisabled] = useState<boolean>(false);

	useEffect(() => {
	}, [kafkaClusterEditingModel.kafkaClusterId]);

	// 表单提交 创建|修改
	const submitForm = async () => {
		await formRef.current?.validateFields();
		confirm({
			title: '确定提交吗?',
			icon: <EditOutlined />,
			content: '增加/编辑集群',
			async onOk() {
        let formObj = formRef.current?.getFieldsFormatValue?.();

        // build data system resource
        const project: API.Project = {
          id: kafkaClusterEditingModel.projectId
        }

        // configurations
        const bootstrapConfiguration: API.DataSystemResourceConfiguration = {
          name: "bootstrap.servers",
          value: formObj!.bootstrapServers
        }
        const securityProtocolConfiguration: API.DataSystemResourceConfiguration = {
          name: "security.protocol",
          value: formObj!.securityProtocol
        }

        let kafkaCluster: API.DataSystemResource = {
          name: formObj.name,
          description: formObj.description,
          projects: [project],
          dataSystemType: "KAFKA",
          resourceType: "KAFKA_CLUSTER",
          dataSystemResourceConfigurations: {
            "bootstrap.servers": bootstrapConfiguration,
            "security.protocol": securityProtocolConfiguration
          }
        }

        if (securityProtocolConfiguration.value != "PLAINTEXT") {
          const saslMechanismConfiguration: API.DataSystemResourceConfiguration = {
            name: "sasl.mechanism",
            value: formObj!.saslMechanism
          }
          const usernameConfiguration: API.DataSystemResourceConfiguration = {
            name: "username",
            value: formObj!.saslUsername
          }
          const passwordConfiguration: API.DataSystemResourceConfiguration = {
            name: "password",
            value: formObj!.saslPassword
          }
          kafkaCluster.dataSystemResourceConfigurations["sasl.mechanism"] = saslMechanismConfiguration;
          kafkaCluster.dataSystemResourceConfigurations["username"] = usernameConfiguration;
          kafkaCluster.dataSystemResourceConfigurations["password"] = passwordConfiguration;
        }

				if ('edit' == kafkaClusterEditingModel.from) {
          kafkaCluster.id = kafkaClusterEditingModel.kafkaClusterId
					await updateDataSystemResource(kafkaCluster)
				}
				else {
					console.log("will create ", kafkaCluster)
					await createDataSystemResource(kafkaCluster)
				}
        tableRef.reload()
				message.info("操作成功")
				setKafkaClusterEditingModel({
					...kafkaClusterEditingModel,
					showModal: false
				})
			},
			onCancel() {},
		});
	}

	// 初始化表单数据
	const initFormValue = async () => {
		if (kafkaClusterEditingModel.from == 'create') {
			return;
		}
		// 表单数据
		let kafkaCluster: API.DataSystemResource = await getDataSystemResource({id: kafkaClusterEditingModel.kafkaClusterId!})

		formRef?.current?.setFieldsValue({
			name: kafkaCluster.name,
			bootstrapServers: kafkaCluster.dataSystemResourceConfigurations["bootstrap.servers"].value,
			description: kafkaCluster.description,
			securityProtocol: kafkaCluster.dataSystemResourceConfigurations["security.protocol"].value,
			saslMechanism: kafkaCluster.dataSystemResourceConfigurations["sasl.mechanism"] ? kafkaCluster.dataSystemResourceConfigurations["sasl.mechanism"].value : "",
			saslUsername: kafkaCluster.dataSystemResourceConfigurations["username"] ? kafkaCluster.dataSystemResourceConfigurations["username"].value : ""
		});

		dynamicSelectionSaslInput(kafkaCluster.dataSystemResourceConfigurations!["security.protocol"].value)
	}

	const dynamicSelectionSaslInput = (securityProtocol: String) => {
		if (securityProtocol === 'PLAINTEXT') {
			setSaslDisabled(true)
			formRef?.current?.setFieldsValue({
				saslMechanism: '',
				saslUsername: '',
				saslPassword: ''
			});

		}
		else {
			setSaslDisabled(false)
		}
	}

	return (
		<ModalForm
			title="集群信息"
			visible={kafkaClusterEditingModel.showModal}
			modalProps={{
				destroyOnClose: true,
				onCancel: () => {
					setKafkaClusterEditingModel({
						...kafkaClusterEditingModel,
						showModal: false
					})
				}
			}}
			onFinish={submitForm}
			onInit={() => {initFormValue()}}
		>
			<ProForm<{
				name?: string;
				saslMechanism?: string;
				saslUsername?: string;
				saslPassword?: string;
			}>
				formRef={formRef}
				formKey="kafkaClusterEditingForm"
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
						name="bootstrapServers"
						label="集群地址"
						placeholder="请输入集群地址"
						rules={[{required: true, message: '集群地址必填'}]}
					/>

				</ProForm.Group>

				<ProForm.Group >
					<ProFormSelect
						name="securityProtocol"
						label="security.protocol"
						width="md"
						options={[
							{
								value: 'SASL_PLAINTEXT',
								label: 'SASL_PLAINTEXT',
							},
							{
								value: 'PLAINTEXT',
								label: 'PLAINTEXT',
							},

						]}
						rules={[{required: true, message: '集群类型必填'}]}
						fieldProps={
							{
								onSelect: (item: string) => {
									dynamicSelectionSaslInput(item);
								}
							}
						}
					/>
					<ProFormSelect
						name="saslMechanism"
						label="sasl.mechanism"
						width="md"
						options={[
							{
								value: 'SCRAM-SHA-512',
								label: 'SCRAM-SHA-512',
							},
							{
								value: 'SCRAM-SHA-256',
								label: 'SCRAM-SHA-256',
							},
							{
								value: 'PLAIN',
								label: 'PLAIN',
							},
						]}
						disabled={saslDisabled}
						hidden={saslDisabled}
						rules={saslDisabled ? [] : [{required: true, message: '必填'}]}
					/>
				</ProForm.Group>

				<ProForm.Group>

					<ProFormText
						width="md"
						name="saslUsername"
						label="sasl.jaas.config.username"
						placeholder="username"
						disabled={saslDisabled}
						hidden={saslDisabled}
						rules={saslDisabled ? [] : [{required: true, message: '必填'}]}
					/>

					<ProFormText
						width="md"
						name="saslPassword"
						label="sasl.jaas.config.password"
						placeholder="passowrd"
						disabled={saslDisabled}
						hidden={saslDisabled}
						rules={saslDisabled ? [] : [{required: true, message: '必填'}]}
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

export default KafkaClusterEditing;
