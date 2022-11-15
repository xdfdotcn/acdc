import React, {useEffect, useRef, useState} from 'react';
import ProForm, {ModalForm, ProFormInstance, ProFormSelect, ProFormText, ProFormTextArea} from '@ant-design/pro-form';
import {useModel} from 'umi';
import {EditOutlined} from '@ant-design/icons';
import {message, Modal} from 'antd';
import {createKafkaCluster, editKafkaCluster, getKafkaCluster} from '@/services/a-cdc/api';
const {confirm} = Modal;

const KafkaClusterEditing: React.FC = () => {
	// 数据流
	const {kafkaClusterEditingModel, setKafkaClusterEditingModel} = useModel('KafkaClusterEditingModel')
	// form ref
	const formRef = useRef<
		ProFormInstance<{
			name: string;
			description?: string;
			version: string;
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
			content: '增加集群',
			async onOk() {
				let formObj = formRef.current?.getFieldsFormatValue?.();
				if ('edit' == kafkaClusterEditingModel.from) {
					let editBody: API.KafkaCluster = {
						id: kafkaClusterEditingModel.kafkaClusterId,
						projectId: kafkaClusterEditingModel.projectId,
						name: formObj!.name,
						clusterType: formObj!.clusterType,
						version: formObj!.version,
						bootstrapServers: formObj!.bootstrapServers,
						securityProtocol: formObj!.securityProtocol,
						saslMechanism: formObj!.saslMechanism,
						saslUsername: formObj!.saslUsername,
						saslPassword: formObj!.saslPassword,
						description: formObj!.description,
					}
					await editKafkaCluster(editBody)
				}
				else {
					let createBody: API.KafkaCluster = {
						id: kafkaClusterEditingModel.kafkaClusterId,
						projectId: kafkaClusterEditingModel.projectId,
						name: formObj!.name,
						version: formObj!.version,
						clusterType: formObj!.clusterType,
						bootstrapServers: formObj!.bootstrapServers,
						securityProtocol: formObj!.securityProtocol,
						saslMechanism: formObj!.saslMechanism,
						saslUsername: formObj!.saslUsername,
						saslPassword: formObj!.saslPassword,
						description: formObj!.description,
					}
					console.log("will create ", createBody)
					await createKafkaCluster(createBody)
				}
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
		let kafkaCluster: API.KafkaCluster = await getKafkaCluster({kafkaClusterId: kafkaClusterEditingModel.kafkaClusterId!})

		formRef?.current?.setFieldsValue({
			name: kafkaCluster.name,
			version: kafkaCluster.version,
			bootstrapServers: kafkaCluster.bootstrapServers,
			description: kafkaCluster.description,
			securityProtocol: kafkaCluster.securityProtocol,
			saslMechanism: kafkaCluster.saslMechanism,
			saslUsername: kafkaCluster.saslUsername,
			saslPassword: kafkaCluster.saslPassword,
		});

		dynamicSelectionSaslInput(kafkaCluster!.securityProtocol!)
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
					<ProFormSelect
						name="clusterType"
						label="集群类型"
						width="md"
						disabled={true}
						options={[
							{
								value: 'USER',
								label: '用户集群',
							},
						]}
						rules={[{required: true, message: '集群类型必填'}]}
					/>
					<ProFormSelect
						name="version"
						label="集群版本"
						width="md"
						disabled={true}
						valueEnum={{
							'2.6.0': '2.6.0',
						}}
						placeholder="请选择集群类型,inner/ticdc/user"
						rules={[{required: true, message: '集群类型必填'}]}
					/>
				</ProForm.Group>
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
