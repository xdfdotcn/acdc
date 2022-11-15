import React, { useEffect, useRef, useState } from 'react';
import type { ProFormInstance } from '@ant-design/pro-form';
import { StepsForm } from '@ant-design/pro-form';
import { message, Modal, Descriptions, Card, Alert, Button } from 'antd';
import Field from '@ant-design/pro-field';
import { history, useModel } from 'umi';
import { applyConnector } from '@/services/a-cdc/api';
const {confirm} = Modal;
import { EditOutlined } from '@ant-design/icons';

import Step1 from './step1';
import Step2 from './step2';
import Step3 from './step3';
// import Step4 from './step4';
import SinkRdbTable from './step4/SinkRdbTable';
import SinkHiveTable from './step4/SinkHiveTable';
import SinkKafka from './step4/SinkKafka';
import Step5 from './step5';
import { handleFieldMappingItem, verifyPrimaryKey } from '@/services/a-cdc/connector/field-mapping';

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
						...applyInfoModel,
						sourceDataSystemType: applyInfoModel?.srcDataSystemType,
            sinkDataSystemType: applyInfoModel?.sinkDataSystemType,
            sourceProjectId: applyInfoModel.srcPrjId,
            sinkProjectId: applyInfoModel?.sinkPrjId,
            sourceDataSetId: applyInfoModel?.srcDataSetId,
            sinkDataSetId: applyInfoModel?.sinkDataSetId,
						sinkDatabaseName: applyInfoModel?.sinkDatabaseName,//目的数据表名
						srcDatabaseName: applyInfoModel?.srcDatabaseName,//源数据表名
            sinkInstanceId: applyInfoModel?.sinkInstanceId,
						connectionColumnConfigurations: newItems,
					}
					if(	applyInfoModel.sinkDataSystemType == "KAFKA" ) {
						newRes.specificConfiguration = JSON.stringify({
							kafkaConverterType: applyInfoModel.sinkKafkaConverterType
						})
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

						connectionColumnConfigurations: newItems
					}

					confirm({
						title: '确定提交吗',
						icon: <EditOutlined />,
						content: '创建链路',
						async onOk() {
							console.log(newRes, 'newRes')
							onSubmit(newRes)
							// await applyConnector({...applyReqBody});
							message.success('提交成功');
							sessionStorage.setItem('applyInfo', JSON.stringify(applyInfoModel))
							// history.push("/connector/connectorMgt")
							setApplyInfoModel({})
							setCurrent(0);
						},
						onCancel() {
							setCurrent(0);
						},
					});

				}}
				formProps={{
					validateMessages: {
						required: '此项为必填项',
					},
				}}
				submitter={{
          render: (props) => {
            if (props.step === 0) {
              return (
                <Button type="primary" onClick={() => props.onSubmit?.()}>
                  下一步 {'>'}
                </Button>
              );
            }

            if (props.step === 1) {
              return [
                <Button key="pre" onClick={() => {props.onPre?.(); setCurrent(0)}}>
                  上一步
                </Button>,
                <Button type="primary" key="goToTree" onClick={() => props.onSubmit?.()}>
                  下一步 {'>'}
                </Button>,
              ];
            }
						if (props.step === 2) {
              return [
                <Button key="pre" onClick={() => {props.onPre?.(); setCurrent(1)}}>
                  上一步
                </Button>,
                <Button type="primary" key="goToTree" onClick={() => props.onSubmit?.()}>
                  下一步 {'>'}
                </Button>,
              ];
            }
						if (props.step === 3) {
              return [
                <Button key="pre" onClick={() => {props.onPre?.(); setCurrent(2)}}>
                  上一步
                </Button>,
                <Button type="primary" key="goToTree" onClick={() => props.onSubmit?.()}>
                  下一步 {'>'}
                </Button>,
              ];
            }
						if (props.step === 4) {
              return [
                <Button key="pre" onClick={() => {props.onPre?.(); setCurrent(3)}}>
                  上一步
                </Button>,
                <Button type="primary" key="goToTree" onClick={() => props.onSubmit?.()}>
                  下一步 {'>'}
                </Button>,
              ];
            }
            return [
              <Button key="gotoTwo" onClick={() => {props.onPre?.(); setCurrent(4)}}>
                {'<'} 上一步
              </Button>,
              <Button type="primary" key="goToTree" onClick={() => props.onSubmit?.()}>
                保存 √
              </Button>,
            ];
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

						if (!applyInfoModel.srcPrjOwnerEmail) {
							message.warn("源项目缺少负责人,请联系ACDC运维老师");
							return false;
						}

						if (!applyInfoModel.srcClusterId) {
							message.warn("请选择集群");
							return false
						}

						setCurrent(1);
						return true;
					}}
				>
					<Step1
						onChange={ onStep1Change }
					/>

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
					<Step2 />
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
					<Step3 />

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
							if (applyInfoModel.sinkDataSystemType == 'HIVE' || applyInfoModel.sinkDataSystemType == 'KAFKA') {
								message.warn("源表不存在主键字段")
							}

							if (applyInfoModel.sinkDataSystemType == 'MYSQL' || applyInfoModel.sinkDataSystemType == 'TIDB') {
								message.warn("源表主键与目标表主键类型不一致")
							}

							return false;
						}
						setCurrent(5);
						return true;
					}}
				>
					<Step5 />
				</StepsForm.StepForm>

				<StepsForm.StepForm
					name="approve"
					title="链路信息确认">
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
