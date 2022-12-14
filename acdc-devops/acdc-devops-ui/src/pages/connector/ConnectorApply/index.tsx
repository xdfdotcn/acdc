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

					// ??????????????????,????????????
					if (!applyInfoModel || !fieldMappingModel.fieldMappings) {
						message.error("?????????????????????");
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
						sinkDatabaseName: applyInfoModel?.sinkDatabaseName,//??????????????????
						srcDatabaseName: applyInfoModel?.srcDatabaseName,//???????????????
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
						title: '???????????????',
						icon: <EditOutlined />,
						content: '????????????',
						async onOk() {
							onSubmit(newRes)
							// await applyConnector({...applyReqBody});
							message.success('????????????');
							sessionStorage.setItem('applyInfo', applyInfoModel)
							// history.push("/connector/connectorMgt")
							setApplyInfoModel({})
						},
						onCancel() {},
					});

				}}
				formProps={{
					validateMessages: {
						required: '??????????????????',
					},
				}}
			>
				<StepsForm.StepForm
					name="sourceCluster"
					title="???????????????"
					onFinish={async () => {
						if (!applyInfoModel.srcPrjId) {
							message.warn("???????????????");
							return false;
						}
						if (!applyInfoModel.srcClusterId) {
							message.warn("???????????????");
							return false
						}

						if (!applyInfoModel.srcPrjId) {
							message.warn("???????????????");
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
					title="????????????"
					onFinish={async () => {
						if (!applyInfoModel.srcDatabaseId) {
							message.warn("??????????????????");
							return false
						}
						if (!applyInfoModel.srcDataSetId) {
							message.warn("??????????????????");
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
					title="??????????????????"
					onFinish={async () => {
						if (!applyInfoModel.sinkPrjId) {
							message.warn("???????????????");
							return false
						}
						if (!applyInfoModel.sinkClusterId) {
							message.warn("???????????????");
							return false
						}
						if (!applyInfoModel.sinkInstanceId) {
							message.warn("???????????????");
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
					title="?????????????????????"
					onFinish={async () => {
						if (applyInfoModel.sinkDataSystemType == 'KAFKA') {
							if (!applyInfoModel.sinkDataSetId) {
								message.warn("????????? topic");
								return false
							}
							if (!applyInfoModel.sinkKafkaConverterType) {
								message.warn("????????????????????????");
								return false
							}
						} else {
							if (!applyInfoModel.sinkDatabaseId) {
								message.warn("??????????????????");
								return false
							}

							if (!applyInfoModel.sinkDataSetId) {
								message.warn("??????????????????");
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
								1. fieldMapping ??????????????????

								2. ????????????,???????????????????????????,????????????????????????id,?????????????????????
								????????????????????????

								3. ?????????????????????,????????????,?????????????????????????????? fieldMappings

								4. ????????????????????? fieldMappings ??????????????????

								5. ????????????useModel ?????????????????????,?????????????????????,?????????????????????
								??????
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
					title="??????????????????"
					onFinish={async () => {

						let fieldMappings: API.FieldMappingListItem[] = fieldMappingModel!.fieldMappings!;

						// ??????????????????
						if (!verifyPrimaryKey(fieldMappings, applyInfoModel.sinkDataSystemType!)) {
							if (applyInfoModel.sinkDataSystemType == 'HIVE'
								|| applyInfoModel.sinkDataSystemType == 'KAFKA'
							) {
								message.warn("???????????????????????????")
							}

							if (applyInfoModel.sinkDataSystemType == 'MYSQL'
								|| applyInfoModel.sinkDataSystemType == 'TIDB'
							) {
								message.warn("?????????????????????????????????????????????")
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
					title="????????????">
					<Card>
						<Alert
							message='????????????'
							type="success"
							showIcon
							banner
							style={{
								margin: -12,
								marginBottom: 24,
							}}
						/>

						<Descriptions column={2}>
							<Descriptions.Item label="?????????">
								<Field text={applyInfoModel!.srcPrjName} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="????????????">
								<Field text={applyInfoModel!.sinkPrjName} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="???????????????">
								<Field text={applyInfoModel!.srcDataSystemType} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="??????????????????">
								<Field text={applyInfoModel!.sinkDataSystemType} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="?????????">
								<Field text={applyInfoModel!.srcClusterName} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="????????????">
								<Field text={applyInfoModel!.sinkClusterName} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="??????">
								<Field text={applyInfoModel!.srcDatabaseName} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="?????????">
								<Field text={applyInfoModel!.sinkDatabaseName} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="??????">
								<Field text={applyInfoModel!.srcDataSetName} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label={applyInfoModel.sinkDataSystemType == 'KAFKA' ? '?????? topic' : '?????????'}>
								<Field text={applyInfoModel!.sinkDataSetName} mode="read" />
							</Descriptions.Item>
							<Descriptions.Item label="????????????">
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
