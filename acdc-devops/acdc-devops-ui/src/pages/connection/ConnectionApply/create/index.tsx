import React, {useEffect, useRef, useState} from 'react';
import type {ProFormInstance} from '@ant-design/pro-form';
import {StepsForm} from '@ant-design/pro-form';
import {message, Modal, Descriptions, Card, Alert, Button} from 'antd';
import Field from '@ant-design/pro-field';
import {useModel} from 'umi';
import {generateConnectionColumnConf} from '@/services/a-cdc/api';
const {confirm} = Modal;
import {EditOutlined} from '@ant-design/icons';

import Step1 from './step1';
import Step2 from './step2';
import Step3 from './step3';
// import Step4 from './step4';
import SinkRdbTable from './step4/SinkRdbTable';
import SinkHiveTable from './step4/SinkHiveTable';
import SinkKafka from './step4/SinkKafka';
import {DataSystemTypeConstant} from '@/services/a-cdc/constant/DataSystemTypeConstant';
import ConnectionColumnConf, {ConnectionColumnConfProps, PageFrom} from '../../components/ConnectionColumnConf';
import {verifyUKWithShowMessage} from '@/services/a-cdc/connection/connection-column-conf-service';
import {EditableFormInstance} from '@ant-design/pro-table';

type ApplyStepFormProps = {
  onSubmit: () => {};
  currentStep: number
}

const ConnectionApply: React.FC<ApplyStepFormProps> = (props) => {
  const {onSubmit, currentStep} = props;
  // 重新定位到分步表单的第几步
  const [current, setCurrent] = useState(0);
  const {applyInfoModel, setApplyInfoModel} = useModel('ConnectionApplyModel')

  const formMapRef = useRef<React.MutableRefObject<ProFormInstance<any> | undefined>[]>([]);

  const [connectionColumnConfPropsState, setConnectionColumnConfPropsState] = useState<ConnectionColumnConfProps>();

  const editorFormRef = useRef<EditableFormInstance<API.ConnectionColumnConf>>();


  useEffect(() => {
    setCurrent(currentStep)

  }, [currentStep]);

  const onStep1Change = () => {

  };

  const chooseSinkDataSetPage = () => {

    let sinkDataSystemType = applyInfoModel.sinkDataSystemType
    // mysql
    if (sinkDataSystemType == DataSystemTypeConstant.MYSQL) {
      return <SinkRdbTable />
    }

    if (sinkDataSystemType == DataSystemTypeConstant.TIDB) {
      return <SinkRdbTable />
    }

    if (sinkDataSystemType == DataSystemTypeConstant.HIVE) {
      return <SinkHiveTable />
    }
    if (sinkDataSystemType == DataSystemTypeConstant.KAFKA) {
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
        onFinish={async () => {
          // 拼接过滤条件,然后执行
          const rows = editorFormRef.current?.getRowsData?.();

          if (!applyInfoModel || !rows) {
            message.error("列映射配置错误");
            return false;
          }

          let newRes = {
            sourceDataSystemType: applyInfoModel?.srcDataSystemType,
            sinkDataSystemType: applyInfoModel?.sinkDataSystemType,
            sourceProjectId: applyInfoModel.srcPrjId,
            sinkProjectId: applyInfoModel?.sinkPrjId,
            sourceDataCollectionId: applyInfoModel?.srcDataCollectionId,
            sinkDataCollectionId: applyInfoModel?.sinkDataCollectionId,
            sinkInstanceId: applyInfoModel?.sinkInstanceId,
            columnConfOriginalData: connectionColumnConfPropsState.originalDataSource!,
            columnConfDisplayData: rows!,
            sourceProjectName: applyInfoModel.srcPrjName,
            sourceDataSystemClusterName: applyInfoModel.srcClusterName,
            sourceDatabaseName: applyInfoModel.srcDatabaseName,
            sourceDataCollectionName: applyInfoModel.srcDataCollectionName,
            sinkProjectName: applyInfoModel.sinkPrjName,
            sinkDataSystemClusterName: applyInfoModel.sinkClusterName,
            sinkDatabaseName: applyInfoModel.sinkDatabaseName,
            sinkDataCollectionName: applyInfoModel.sinkDataCollectionName,
          }

          if (applyInfoModel.sinkDataSystemType == "KAFKA") {
            newRes.specificConfiguration = JSON.stringify({
              dataFormatType: applyInfoModel.sinkKafkaConverterType
            })
          }
          confirm({
            title: '确定加入申请单吗',
            icon: <EditOutlined />,
            content: '批量申请链路',
            async onOk() {
              onSubmit(newRes)
              sessionStorage.setItem('applyInfo', JSON.stringify(applyInfoModel))
              setApplyInfoModel({})
              setCurrent(0);
            },
            onCancel() {
              setCurrent(0);
            },
          });

          return true;

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
              message.warn("请选择源项目");
              return false;
            }

            if (!applyInfoModel.srcPrjOwnerName) {
              message.warn("源项目未配置负责人,请联系ACDC运维老师");
              return false;
            }

            if (!applyInfoModel.srcClusterId) {
              message.warn("请选择源集群");
              return false
            }

            setCurrent(1);
            return true;
          }}
        >
          <Step1
            onChange={onStep1Change}
          />

        </StepsForm.StepForm>
        {/* step2 */}
        <StepsForm.StepForm
          name="sourceRdbTalbe"
          title="选择源表"
          onFinish={async () => {
            if (!applyInfoModel.srcDatabaseId) {
              message.warn("请选择源数据库");
              return false
            }
            if (!applyInfoModel.srcDataCollectionId) {
              message.warn("请选择源数据表");
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
              message.warn("请选择目标项目");
              return false
            }
            if (!applyInfoModel.sinkClusterId) {
              message.warn("请选择目标集群");
              return false
            }

            if (applyInfoModel.sinkDataSystemType == DataSystemTypeConstant.MYSQL
              || applyInfoModel.sinkDataSystemType == DataSystemTypeConstant.TIDB
            ) {
              if (!applyInfoModel.sinkInstanceId) {
                message.warn("请选择目标实例");
                return false
              }
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
              if (!applyInfoModel.sinkDataCollectionId) {
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

              if (!applyInfoModel.sinkDataCollectionId) {
                message.warn("请选择数据表");
                return false
              }
            }

            setCurrent(4);
            // 1. 加载字段映射数据源
            let srcDataCollectionId = applyInfoModel?.srcDataCollectionId!
            let sinkDataCollectionId = applyInfoModel?.sinkDataCollectionId!
            let dataSource = await generateConnectionColumnConf(srcDataCollectionId, sinkDataCollectionId)

            // 2. 更新columnConf组件数据
            setConnectionColumnConfPropsState({
              displayDataSource: dataSource,
              originalDataSource: dataSource,
              canEdit: applyInfoModel.sinkDataSystemType != DataSystemTypeConstant.KAFKA,
              canDelete: applyInfoModel.sinkDataSystemType == DataSystemTypeConstant.KAFKA,
              sinkDataSystemType: applyInfoModel.sinkDataSystemType,
              sourceDataCollectionId: applyInfoModel.srcDataCollectionId
            })

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
            let columnConfList = editorFormRef.current?.getRowsData?.();

            let sinkDataSystemType = applyInfoModel.sinkDataSystemType

            if (!verifyUKWithShowMessage(columnConfList, sinkDataSystemType!)) {
              return false
            }

            setCurrent(5);
            return true;
          }}
        >
          <ConnectionColumnConf
            columnConfProps={{...connectionColumnConfPropsState}}
            editorFormRef={editorFormRef}
          />
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
                <Field text={applyInfoModel!.srcDataCollectionName} mode="read" />
              </Descriptions.Item>
              <Descriptions.Item label={applyInfoModel.sinkDataSystemType == 'KAFKA' ? '目标 topic' : '目标表'}>
                <Field text={applyInfoModel!.sinkDataCollectionName} mode="read" />
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

export default ConnectionApply
