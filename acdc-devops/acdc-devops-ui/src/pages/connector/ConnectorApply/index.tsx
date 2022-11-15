import React, { useEffect, useRef, useState } from 'react';
import type { ProFormInstance } from '@ant-design/pro-form';
import { StepsForm } from '@ant-design/pro-form';
import { message, Modal, Descriptions, Card, Alert } from 'antd';
import Field from '@ant-design/pro-field';
import { history, useModel } from 'umi';
import { applyConnector } from '@/services/a-cdc/api';
const {confirm} = Modal;
import { EditOutlined } from '@ant-design/icons';
import SourceCluster from './components/SourceCluster';
import SinkRdbTable from './components/SinkRdbTable';
import SinkHiveTable from './components/SinkHiveTable';
import SinkCluster from './components/SinkCluster';
import SourceTable from './components/SourceTable';
import FieldMappingList from '../components/FieldMapping';
import { handleFieldMappingItem, verifyPrimaryKey } from '@/services/a-cdc/connector/field-mapping';
import SinkKafka from './components/SinkKafka';
type ApplyStepFormProps = {
	onSubmit: () => {};
	currentStep: number
}

const ConnectorApply: React.FC<ApplyStepFormProps> = (props) => {
	const { onSubmit, currentStep } = props;
	const [current, setCurrent] = useState(0);
	const {applyInfoModel,setApplyInfoModel} = useModel('ConnectorApplyModel')

	const { fieldMappingModel, setFieldMappingModel } = useModel('FieldMappingModel');
	const formMapRef = useRef<React.MutableRefObject<ProFormInstance<any> | undefined>[]>([]);
	useEffect(() => {
		setCurrent(currentStep)
	},[currentStep]);

	const onStep1Change = () => {

	};

	const chooseSinkDataSetPage = () => {

		let sinkDataSystemType = applyInfoModel.sinkDataSystemType
		// mysql
		if (sinkDataSystemType == 'MYSQL') {
			return <SinkRdbTable />
		}

		if (sinkDataSystemType == 'TIDB') {
			return <SinkRdbTable />
		}

		if (sinkDataSystemType == 'HIVE') {
			return <SinkHiveTable />
		}
		if (sinkDataSystemType == 'KAFKA') {
			return <SinkKafka />
		}

		return <></>
	}

	return (
		<>
			<StepsForm
				current={current}
				containerStyle={{width: '100%', height: '100%'}}
				formMapRef={formMapRef}
				onFinish={async (values) => {

					// 拼接过滤条件,然后执行
					if (!applyInfoModel || !fieldMappingModel.fieldMappings) {
						message.error("错误的字段映射");
						return false;
					}
					let newItems = handleFieldMappingItem(fieldMappingModel?.fieldMappings);

					let newRes =  {
						sourceDataSystemType: applyInfoModel?.srcDataSystemType,
            sinkDataSystemType: applyInfoModel?.sinkDataSystemType,
            sourceProjectId: applyInfoModel.srcPrjId,
            sinkProjectId: applyInfoModel?.sinkPrjId,
            sourceDataSetId: applyInfoModel?.srcDataSetId,
            sinkDataSetId: applyInfoModel?.sinkDataSetId,
						sinkDatabaseName: applyInfoModel?.sinkDatabaseName,//目的数据表名
						srcDatabaseName: applyInfoModel?.srcDatabaseName,//源数据表名
            sinkInstanceId: applyInfoModel?.sinkInstanceId,
						fieldMappings: newItems,
						specificConfiguration: applyInfoModel?.specificConfiguration,
					}
					let applyReqBody: API.ConnectorApplyInfo = {
						sourceDataSystemType: applyInfoModel?.srcDataSystemType,
						sinkDataSystemType: applyInfoModel?.sinkDataSystemType,
						sourceCreationInfo: {
							clusterId: applyInfoModel?.srcClusterId,
							instanceId: -99,
							databaseId: applyInfoModel?.srcDatabaseId,
							dataSetId: applyInfoModel?.srcDataSetId,
							sourceProjectId: applyInfoModel.srcPrjId
						},
						sinkCreationInfo: {
							clusterId: applyInfoModel?.sinkClusterId,
							instanceId: applyInfoModel?.sinkInstanceId,
							databaseId: applyInfoModel?.sinkDatabaseId,
							dataSetId: applyInfoModel?.sinkDataSetId,
							kafkaConverterType: applyInfoModel?.sinkKafkaConverterType,
							sinkProjectId: applyInfoModel?.sinkPrjId,
						},

						fieldMappings: newItems
					}

					confirm({
						title: '确定提交吗',
						icon: <EditOutlined />,
						content: '创建链路',
						async onOk() {
							onSubmit(newRes)
							// await applyConnector({...applyReqBody});
							message.success('提交成功');
							sessionStorage.setItem('applyInfo', applyInfoModel)
							// history.push("/connector/connectorMgt")
							setApplyInfoModel({})
						},
						onCancel() {},
					});

				}}
				formProps={{
					validateMessages: {
						required: '此项为必填项',
					},
				}}
			>
				<StepsForm.StepForm
					name="sourceCluster"
					title="选择源集群"
					onFinish={async () => {
						if (!applyInfoModel.srcPrjId) {
							message.warn("请选择项目");
							return false;
						}
						if (!applyInfoModel.srcClusterId) {
							message.warn("请选择集群");
							return false
						}

						if (!applyInfoModel.srcPrjId) {
							message.warn("请选择项目");
							return false;
						}
						setCurrent(1);
						return true;
					}}
				>
					<SourceCluster />

				</StepsForm.StepForm>
				{/* step2 */}
				<StepsForm.StepForm
					name="sourceRdbTalbe"
					title="选择源表"
					onFinish={async () => {
						if (!applyInfoModel.srcDatabaseId) {
							message.warn("请选择数据库");
							return false
						}
						if (!applyInfoModel.srcDataSetId) {
							message.warn("请选择数据表");
							return false
						}
						setCurrent(2);
						return true;
					}}
				>
					<SourceTable />
				</StepsForm.StepForm>
				{/* step3 */}
				<StepsForm.StepForm
					name="sinkCluster"
					title="选择目标集群"
					onFinish={async () => {
						if (!applyInfoModel.sinkPrjId) {
							message.warn("请选择项目");
							return false
						}
						if (!applyInfoModel.sinkClusterId) {
							message.warn("请选择集群");
							return false
						}
						if (!applyInfoModel.sinkInstanceId) {
							message.warn("请选择实例");
							return false
						}
						setCurrent(3);
						return true;
					}}
				>
					<SinkCluster />

				</StepsForm.StepForm>
				{/* step4 */}
				<StepsForm.StepForm
					name="sinkDataSet"
					title="选择目标数据集"
					onFinish={async () => {
						if (applyInfoModel.sinkDataSystemType == 'KAFKA') {
							if (!applyInfoModel.sinkDataSetId) {
								message.warn("请选择 topic");
								return false
							}
							if (!applyInfoModel.sinkKafkaConverterType) {
								message.warn("请选择序列化方式");
								return false
							}
						} else {
							if (!applyInfoModel.sinkDatabaseId) {
								message.warn("请选择数据库");
								return false
							}

							if (!applyInfoModel.sinkDataSetId) {
								message.warn("请选择数据表");
								return false
							}
						}

						setFieldMappingModel({
							from: 'apply',
							srcDataSetId: applyInfoModel!.srcDataSetId!,
							srcDataSystemType: applyInfoModel.srcDataSystemType!,
							srcClusterType: applyInfoModel.srcClusterType!,
							sinkDataSystemType: applyInfoModel!.sinkDataSystemType!,
							sinkClusterType: applyInfoModel!.sinkClusterType!,
							sinkDataSetId: applyInfoModel!.sinkDataSetId!,
							/**
								1. fieldMapping 组件绑定请求

								2. 上一步的,在回到下一步的时候,如果没有该更选中id,则不会触发组件
								重新请求渲染数据

								3. 如果不请求数据,重新渲染,则不能保存到状态中的 fieldMappings

								4. 所有要把之前的 fieldMappings 重新保存起来

								5. 经过测试useModel 不支持差异更新,每次都全量覆盖,所以需要把历史
								保留
							*/
							fieldMappings: fieldMappingModel?.fieldMappings
						})
						setCurrent(4);
						return true;
					}}
				>
					{chooseSinkDataSetPage()}

				</StepsForm.StepForm>
				{/* step5 */}
				<StepsForm.StepForm
					name="fieldMapping"
					title="配置字段映射"
					onFinish={async () => {

						let fieldMappings: API.FieldMappingListItem[] = fieldMappingModel!.fieldMappings!;

						// 主键字段校验
						if (!verifyPrimaryKey(fieldMappings, applyInfoModel.sinkDataSystemType!)) {
							if (applyInfoModel.sinkDataSystemType == 'HIVE'
								|| applyInfoModel.sinkDataSystemType == 'KAFKA'
							) {
								message.warn("源表不存在主键字段")
							}

							if (applyInfoModel.sinkDataSystemType == 'MYSQL'
								|| applyInfoModel.sinkDataSystemType == 'TIDB'
							) {
								message.warn("源表主键与目标表主键类型不一致")
							}

							return false;
						}
						setCurrent(5);
						return true;
					}}
				>
					<FieldMappingList />
				</StepsForm.StepForm>

				<StepsForm.StepForm
					name="approve"
					title="提交审批">
					<Card>
						<Alert
							message='链路信息'
							type="success"
							showIcon
							banner
							style={{
								margin: -12,
								marginBottom: 24,
							}}
						/>

						<Descriptions column={2}>
							<Descriptions.Item label="源项目">
								<Field text={applyInfoModel!.srcPrjName} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="目标项目">
								<Field text={applyInfoModel!.sinkPrjName} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="源集群类型">
								<Field text={applyInfoModel!.srcDataSystemType} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="目标集群类型">
								<Field text={applyInfoModel!.sinkDataSystemType} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="源集群">
								<Field text={applyInfoModel!.srcClusterName} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="目标集群">
								<Field text={applyInfoModel!.sinkClusterName} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="源库">
								<Field text={applyInfoModel!.srcDatabaseName} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="目标库">
								<Field text={applyInfoModel!.sinkDatabaseName} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="原表">
								<Field text={applyInfoModel!.srcDataSetName} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label={applyInfoModel.sinkDataSystemType == 'KAFKA' ? '目标 topic' : '目标表'}>
								<Field text={applyInfoModel!.sinkDataSetName} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="目标实例">
								<Field text={applyInfoModel!.sinkInstanceName} mode="read" />
							</Descriptions.Item>
						</Descriptions>
					</Card>
					<br></br>
					<br></br>
					<br></br>
				</StepsForm.StepForm>

			</StepsForm>
		</>
	);
};

export default ConnectorApply
