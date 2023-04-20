import React, { useRef, useState } from "react";
import { Modal, Table, Input, Button, Popconfirm, message, Drawer, Space, Spin } from 'antd';
import styles from './index.less';
import ConnectionCreateItem from './create';
import { applyConnection } from '@/services/a-cdc/api';
import { history, useModel } from 'umi';
import ConnectionColumnConf, { ConnectionColumnConfProps, PageFrom } from "../components/ConnectionColumnConf";
import { DrawerForm } from "@ant-design/pro-form";
import { EditOutlined } from "@ant-design/icons";
import { DataSystemTypeConstant } from "@/services/a-cdc/constant/DataSystemTypeConstant";
import { verifyUKWithShowMessage } from "@/services/a-cdc/connection/connection-column-conf-service";
import { EditableFormInstance } from "@ant-design/pro-table";

const { confirm } = Modal;

const { TextArea } = Input;

const ConnectionApply: React.FC<{}> = () => {
  const { applyInfoModel, setApplyInfoModel } = useModel('ConnectionApplyModel');
  const { connectionColumnConfModel, setConnectionColumnConfModelData } = useModel('ConnectionColumnConfModel');
  const [loading, setLoading] = useState(false);
  const [dataSource, setDataSource] = useState<any[]>([]);
  const [drawerVisible, setDrawerVisible] = useState(false);
  const [value, setValue] = useState('');
  const [idx, setidx] = useState(1);
  const [step, setStep] = useState(0);
  const [configModalVisible, handleModalVisible] = useState(false);
  const [selectItem, setSelectItem] = useState<any>({});
  const [selectKey, setSelectKey] = useState<any>({});

  const [connectionColumnConfPropsState, setConnectionColumnConfPropsState] = useState<ConnectionColumnConfProps>();

  const editorFormRef = useRef<EditableFormInstance<API.ConnectionColumnConf>>();

  const onDrawerClose = () => {
    sessionStorage.removeItem('applyInfo');
    setDrawerVisible(false);
    setStep(0);
  };
  const handleAdd = () => {
    if (sessionStorage.getItem('applyInfo')) {
      setApplyInfoModel(JSON.parse(sessionStorage.getItem('applyInfo')))
    }
    setStep(0);
    setDrawerVisible(true);
  };
  const handleDelete = (record) => {
    setDataSource(dataSource.filter((item) => item.id !== record.id));
  };

  const handleSubmit = async () => {
    let connections: API.ConnectionDetail[] = []

    dataSource.forEach((element, _index, _arr) => {
      let connectionColumnConfigurations: API.ConnectionColumnConf[] = []
      let columnConfDisplayData = element.columnConfDisplayData as API.ConnectionColumnConf[]
      columnConfDisplayData!.forEach((record, _index, _arr) => {
        connectionColumnConfigurations.push({
          sourceColumnName: record.sourceColumnName,
          sourceColumnType: record.sourceColumnType,
          sourceColumnUniqueIndexNames: record.sourceColumnUniqueIndexNames,
          sinkColumnName: record.sinkColumnName,
          sinkColumnType: record.sinkColumnType,
          sinkColumnUniqueIndexNames: record.sinkColumnUniqueIndexNames,
          filterOperator: record.filterOperator,
          filterValue: record.filterValue,
        })
      })

      connections.push({
        sourceDataSystemType: element.sourceDataSystemType!,
        sourceProjectId: element.sourceProjectId!,
        sourceDataCollectionId: element.sourceDataCollectionId!,
        sinkDataCollectionId: element.sinkDataCollectionId!,
        sinkDataSystemType: element.sinkDataSystemType!,
        sinkProjectId: element.sinkProjectId!,
        specificConfiguration: element.specificConfiguration!,
        sinkInstanceId: element.sinkInstanceId,
        connectionColumnConfigurations: connectionColumnConfigurations!
      })
    })

    let reqBody: API.ConnectionRequisitionDetail = {
      connections: connections,
      description: value,
    }

    confirm({
      title: '确定提交吗',
      icon: <EditOutlined />,
      content: '链路申请单',
      async onOk() {

        setLoading(true)
        // 自定义 http response 处理
        // issues/935   https://github.com/ant-design/ant-design-pro/issues/935

        fetch(new Request('/api/v1/connections', {
          method: 'post',
          headers: {
            'Content-Type': 'application/json;charset=utf-8'
          },
          body: JSON.stringify(reqBody)
        }))
          .then(response => { // promise

            if (response.status == 200) {
              setLoading(false)
              message.success('提交成功');
              history.push("/connection/connection-mgt")
              return
            }

            if (response.status == 409) {
              response.json().then(jsonBody => {
                setLoading(false)
                message.warn(jsonBody.errorMessage);
              })
            } else {
              response.json().then(jsonBody => {
                message.error(jsonBody.errorMessage);
              })
            }

          })
      },
      onCancel() {

      },
    });
  }

  const getPath = (name?: string) => {
    if (!name || name == '') {
      return ''
    }
    return "/" + name
  }

  const defaultColumns = [
    {
      title: '源端路径',
      dataIndex: 'sourcePath',
    },
    {
      title: '目标端路径',
      dataIndex: 'sinkPath',
    },
    {
      title: '操作',
      dataIndex: 'approvalOperation',
      render: (_, record, idx: { key: React.Key }) =>
        <>
          {dataSource.length >= 1 ? (
            <Popconfirm title="确定删除?" onConfirm={() => handleDelete(record)}>
              <Button type="link">删除</Button>
            </Popconfirm>
          ) : null}
          <Button
            type="link"
            onClick={() => {
              setSelectItem(record);
              setSelectKey(idx);
              setConnectionColumnConfPropsState({
                displayDataSource: record!.columnConfDisplayData,
                originalDataSource: record!.columnConfOriginalData!,
                canEdit: record.sinkDataSystemType != DataSystemTypeConstant.KAFKA,
                canDelete: record.sinkDataSystemType == DataSystemTypeConstant.KAFKA,
                sinkDataSystemType: record.sinkDataSystemType,
                sourceDataCollectionId: record!.sourceDataCollectionId!
              })

              handleModalVisible(true);

            }}
          >编辑</Button>
        </>
    },
  ]
  return (
    <Spin spinning={loading}>
      <div className={styles.container}>
        <div className={styles.action}>
          <Button onClick={handleAdd} type="primary" style={{ marginBottom: 16 }}>
            新增链路
          </Button>
        </div>

        <Table
          columns={defaultColumns}
          dataSource={dataSource}
          rowKey="id"
          pagination={false}
        />
        <div>
          <TextArea
            value={value}
            onChange={e => setValue(e.target.value)}
            placeholder="请输入申请理由"
            autoSize={{ minRows: 3, maxRows: 10 }}
          />
          <Space>
            <Button
              type="primary"
              style={{ marginTop: 16 }}
              onClick={
                async () => {
                  if (!dataSource || dataSource.length <= 0) {
                    message.warn("请添加需要申请的链路")
                    return
                  }

                  let len = value?.replace(/(^\s*)|(\s*$)/g, "").length;
                  if (!value || len <= 0) {
                    message.warn("请输入申请理由")
                    return;
                  }

                  handleSubmit()
                }
              }
            >提交</Button>
            <Button
              style={{ marginTop: 16 }}
              onClick={() => {
                history.push("/connection/connection-mgt")
              }}
            >返回</Button>
          </Space>
        </div>
        <Drawer
          title="新增数据"
          // placement={placement}
          width={"100%"}
          onClose={onDrawerClose}
          visible={drawerVisible}
        >
          <ConnectionCreateItem
            currentStep={step}
            onSubmit={(res) => {

              let sourcePath = res.sourceProjectName
                + getPath(res.sourceDataSystemClusterName)
                + getPath(res.sourceDatabaseName)
                + getPath(res.sourceDataCollectionName)

              let sinkPath = res.sinkProjectName
                + getPath(res.sinkDataSystemClusterName)
                + getPath(res.sinkDatabaseName)
                + getPath(res.sinkDataCollectionName)

              setDataSource(dataSource.concat([{
                ...res,
                id: idx,
                sourcePath: sourcePath,
                sinkPath: sinkPath
              }]));
              setidx(idx + 1);
              onDrawerClose()
            }}
          />
        </Drawer>

        <DrawerForm<{
        }>
          title="字段映射"
          visible={configModalVisible}
          width={"100%"}
          drawerProps={{
            forceRender: false,
            destroyOnClose: true,
            onClose: () => {
              handleModalVisible(false)
            },
          }}
          onFinish={async () => {
            let sinkDataSystemType = dataSource[selectKey].sinkDataSystemType!
            //let columnConfList = connectionColumnConfModel.currentData!
            let rows = editorFormRef.current?.getRowsData?.();
            let columnConfList: API.ConnectionColumnConf[] = []
            for (let row of rows!) {
              columnConfList.push({ ...row })
            }

            if (!verifyUKWithShowMessage(columnConfList!, sinkDataSystemType!)) {
              return
            }

            const copy = dataSource;
            dataSource[selectKey] = {
              ...dataSource[selectKey],
              columnConfDisplayData: columnConfList,
            }

            const newDataSource = []
            for (let it of dataSource) {
              newDataSource.push({ ...it })
            }

            setDataSource(newDataSource)
            handleModalVisible(false);
            setSelectKey('');
            setSelectItem({});
            handleModalVisible(false)
          }}
          onInit={() => { }}
        >
          <ConnectionColumnConf
            columnConfProps={{
              ...connectionColumnConfPropsState
            }}
            editorFormRef={editorFormRef}
          />
        </DrawerForm>
      </div>
    </Spin>
  );
}

export default ConnectionApply;
