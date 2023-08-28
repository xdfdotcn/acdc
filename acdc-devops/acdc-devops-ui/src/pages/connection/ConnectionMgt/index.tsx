import React, { useRef, useState } from 'react';
import type { EditableFormInstance, ProColumns } from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import Field from '@ant-design/pro-field';

import {
  deleteConnection,
  updateConnection,
  editConnectionStatus,
  generateConnectionColumnConfByConnectionId,
  getConnectionActualStatus,
  getConnectionDetail,
  queryConnection,
  stopConnection,
  startConnection,
} from '@/services/a-cdc/api';
import { message, Drawer, Modal, Tag, Descriptions, Space, Button, Divider } from 'antd';
import { useAccess, useModel } from 'umi';
import { DrawerForm } from '@ant-design/pro-form';
import {
  EditOutlined,
  PoweroffOutlined,
  PlaySquareOutlined,
  CopyOutlined,
  StopOutlined,
} from '@ant-design/icons';
import ConnectionColumnConf, {
  ConnectionColumnConfProps,
  PageFrom,
} from '../components/ConnectionColumnConf';
import ConnectionInfo from '../ConnectionInfo';
import ConnectorDetail from '@/pages/connector/ConnectorMgt/components/ConnectorDetail';
import { DataSystemTypeConstant } from '@/services/a-cdc/constant/DataSystemTypeConstant';
const { confirm } = Modal;

const ConnectionList: React.FC = () => {
  // 全局数据流
  const { connectionModel, setConnectionModel } = useModel('ConnectionModel');

  const [connectionDetail, setConnectionDetail] = useState<API.ConnectionDetail>({});

  const [showDetail, setShowDetail] = useState<boolean>(false);

  const [showEditState, setShowEditState] = useState<boolean>(false);

  const [connectorId, setConnectorId] = useState<number>();

  const [showConnectorDetail, setShowConnectorDetail] = useState<boolean>(false);

  const access = useAccess();

  const [connectionColumnConfPropsState, setConnectionColumnConfPropsState] =
    useState<ConnectionColumnConfProps>();

  const editorFormRef = useRef<EditableFormInstance<API.ConnectionColumnConf>>();

  const runStateValueEnum = {
    STARTING: { text: '启动中', status: 'STARTING' },
    RUNNING: { text: '运行中', status: 'RUNNING' },
    STOPPING: { text: '停止中', status: 'STOPPING' },
    STOPPED: { text: '停止', status: 'STOPPED' },
    FAILED: { text: '运行失败', status: 'FAILED' },
  };

  const requisitionStateValueEnum = {
    APPROVING: { text: '审批中', status: 'APPROVING' },
    REFUSED: { text: '审批拒绝', status: 'REFUSED' },
    APPROVED: { text: '审批通过', status: 'APPROVED' },
  };

  /**
		提交修改表单
	*/
  const handleUpdateConnection = async () => {
    confirm({
      title: '确定提交吗',
      icon: <EditOutlined />,
      content: '修改字段映射',
      async onOk() {
        let connectionColumnConfigurations: API.ConnectionColumnConf[] = [];
        const rows = editorFormRef.current?.getRowsData?.();
        rows!.forEach((record, _index, _arr) => {
          connectionColumnConfigurations.push({
            sourceColumnName: record.sourceColumnName,
            sourceColumnType: record.sourceColumnType,
            sourceColumnUniqueIndexNames: record.sourceColumnUniqueIndexNames,
            sinkColumnName: record.sinkColumnName,
            sinkColumnType: record.sinkColumnType,
            sinkColumnUniqueIndexNames: record.sinkColumnUniqueIndexNames,
            filterOperator: record.filterOperator,
            filterValue: record.filterValue,
          });
        });

        let connections: API.ConnectionDetail[] = [];
        connections.push({
          id: connectionModel.connectionId,
          connectionColumnConfigurations: connectionColumnConfigurations!,
        });

        await updateConnection(connections);
        message.success('修改成功');
        setConnectionModel({
          ...connectionModel,
          refreshVersion: connectionModel.refreshVersion! + 1,
          showEdit: false,
        });
        setShowEditState(false);
      },
      onCancel() {},
    });
  };

  /**
	 更新connection 状态
	*/
  const startOrStopConnection = (
    connectionId?: number,
    state?: string,
    requisitionState?: string,
  ) => {
    if ('APPROVED' != requisitionState) {
      message.warn('您申请的链路还未审批通过,不能启停链路');
      return;
    }

    confirm({
      title: '确定操作吗',
      icon: <EditOutlined />,
      content: '修改链路状态',
      async onOk() {
        // 处理条件过滤
        if (state == 'STOPPED') {
          await stopConnection(connectionId!);
        } else {
          await startConnection(connectionId!);
        }
        message.success('操作成功');
        setConnectionModel({
          ...connectionModel,
          refreshVersion: connectionModel.refreshVersion! + 1,
        });
      },
      onCancel() {},
    });
  };

  /**
	 删除 connection
	*/
  const delConnection = async (connectionId?: number) => {
    // 必须停止链路才可以修改,可能会触发审批
    let status = await getConnectionActualStatus({ id: connectionId });

    if (status != 'STOPPED') {
      message.warn('请停止链路');
      return;
    }

    confirm({
      title: '确定删除吗?',
      icon: <EditOutlined />,
      content: '删除链路',
      async onOk() {
        // 处理条件过滤
        await deleteConnection({
          connectionId: connectionId,
        });
        message.success('操作成功');
        setConnectionModel({
          ...connectionModel,
          refreshVersion: connectionModel.refreshVersion! + 1,
        });
      },
      onCancel() {},
    });
  };

  const handleConnectionDetail = async (record: API.Connection) => {
    let detail = await getConnectionDetail(record.id!);

    setConnectionDetail(detail!);
    setShowDetail(true);
  };

  const editConnectionColumnConf = async (record: API.Connection) => {
    // 必须停止链路才可以修改,可能会触发审批
    let status = await getConnectionActualStatus({ id: record.id });

    if ('APPROVED' != record.requisitionState) {
      message.warn('您申请的链路还未审批通过,不能编辑配置');
      return;
    }
    if (status != 'STOPPED') {
      message.warn('请停止链路');
      return;
    }

    let connectionColumnConfs: API.ConnectionColumnConf[] =
      await generateConnectionColumnConfByConnectionId(record.id!);

    setConnectionColumnConfPropsState({
      displayDataSource: connectionColumnConfs,
      originalDataSource: connectionColumnConfs,
      canEdit: record.sinkDataSystemType != DataSystemTypeConstant.KAFKA,
      canDelete: record.sinkDataSystemType == DataSystemTypeConstant.KAFKA,
      sinkDataSystemType: record.sinkDataSystemType,
      sourceDataCollectionId: record.sourceDataCollectionId,
    });

    setShowEditState(true);

    setConnectionModel({
      ...connectionModel,
      connectionId: record.id,
      sinkDataSystemType: record.sinkDataSystemType,
      showEdit: true,
    });
  };

  const doShowConnectorDetail = (connectorId: number) => {
    setConnectorId(connectorId);
    setShowConnectorDetail(true);
  };

  const connectionColumns: ProColumns<API.Connection>[] = [
    {
      title: '目标数据集',
      width: '35%',
      dataIndex: 'sinkDataCollectionName',
      ellipsis: true,
      copyable: true,
    },
    {
      title: '源数据集',
      width: '23%',
      dataIndex: 'sourceDataCollectionName',
      ellipsis: true,
      copyable: true,
    },
    {
      title: '数据系统类型',
      width: '8%',
      dataIndex: 'sinkDataSystemType',
      ellipsis: true,
      onFilter: true,
      valueType: 'select',
      valueEnum: {
        MYSQL: { text: 'MYSQL', status: 'Success' },
        TIDB: { text: 'TIDB', status: 'Success' },
        HIVE: { text: 'HIVE', status: 'Success' },
        KAFKA: { text: 'KAFKA', status: 'Success' },
        ELASTICSEARCH: { text: 'ELASTICSEARCH', status: 'Success' },
        STARROCKS: { text: 'STARROCKS', status: 'Success' },
      },
    },
    {
      title: '运行状态',
      width: '6%',
      dataIndex: 'actualState',
      valueType: 'select',
      valueEnum: runStateValueEnum,
    },
    {
      title: '创建时间',
      width: '13%',
      dataIndex: 'creationTime',
      search: false,
    },
    {
      title: '操作',
      width: '15%',
      valueType: 'option',
      dataIndex: 'option',
      render: (text, record, _, action) => [
        <a
          key={'info_' + record.id}
          onClick={() => {
            handleConnectionDetail(record!);
          }}
        >
          <CopyOutlined />
          详情
        </a>,
        <a
          key={'editable_' + record.id}
          onClick={() => {
            editConnectionColumnConf(record);
          }}
        >
          <EditOutlined />
          编辑
        </a>,
        <>
          {
            record.requisitionState != 'APPROVING' ? (
              record.desiredState == 'RUNNING' ? (
                <a
                  key={'stop_' + record.id}
                  onClick={() => {
                    startOrStopConnection(record.id, 'STOPPED', record.requisitionState);
                  }}
                >
                  <PlaySquareOutlined />
                  停止
                </a>
              ) : (
                <a
                  key={'start_' + record.id}
                  onClick={() => {
                    startOrStopConnection(record.id, 'RUNNING', record.requisitionState);
                  }}
                >
                  <PoweroffOutlined />
                  启动
                </a>
              )
            ) : (
              <a style={{cursor: "wait"}}>
                <StopOutlined />
                { requisitionStateValueEnum[record.requisitionState!].text }
              </a>
            )
          }
        </>,
        <>
          {}
        </>,
      ],
    },
  ];

  /**
   * 生成数据集的展示路径
   *
   * @param dataCollectionResourcePath  数据集资源路径列表
   * @returns 展示路径
   */
  const generateDataCollectionResourcePath = (
    dataCollectionResourcePath: API.DataSystemResource[],
  ) => {
    const path: JSX.Element[] = [];
    {
      dataCollectionResourcePath?.map((item, index) => {
        path.push(
          <>
            <strong>{item.resourceType}</strong>&nbsp;
            <span>{item.name}</span>&nbsp;&nbsp;/&nbsp;&nbsp;
          </>,
        );
      });
    }
    return path;
  };
  /**
   * 列表二级展示内容
   *
   * @param record 数据行记录
   * @returns 二级展示内容表格
   */
  const expandedRowRender = (record: API.Connection) => {
    const expandedDataSource = [];
    expandedDataSource.push(record);
    return (
      <ProTable<API.Connection>
        columns={[
          {
            title: '',
            dataIndex: 'id',
            key: 'id',
            render: (_text, record, _, _action) => [
              <>
                <div>
                  <span>源端路径:</span>&nbsp;&nbsp;/&nbsp;&nbsp;
                  <strong>{'PROJECT'}</strong>&nbsp;
                  <span>{record.sourceProjectName}</span>&nbsp;&nbsp;/&nbsp;&nbsp;
                  {generateDataCollectionResourcePath(record.sourceDataCollectionResourcePath!)}
                </div>

                <Divider />

                <div>
                  <span>目标路径:</span>&nbsp;&nbsp;/&nbsp;&nbsp;
                  <strong>{'PROJECT'}</strong>&nbsp;
                  <span>{record.sinkProjectName}</span>&nbsp;&nbsp;/&nbsp;&nbsp;
                  {generateDataCollectionResourcePath(record.sinkDataCollectionResourcePath!)}
                </div>

                <Divider />

                <div>
                  <span>申请人:</span>&nbsp;&nbsp;
                  <span> {record.userEmail}</span>
                </div>

                <Divider />

                <Tag color="cyan">{record.sinkDataSystemType}</Tag>
                <Tag color="cyan">{runStateValueEnum[record.actualState!].text}</Tag>
                <Tag color="cyan">{requisitionStateValueEnum[record.requisitionState!].text}</Tag>

                {access.canAdmin ? <Divider /> : <></>}

                <Space className="site-button-ghost-wrapper">
                  <>
                    {access.canAdmin ? (
                      <Button
                        type="dashed"
                        ghost
                        onClick={() => {
                          doShowConnectorDetail(record.sinkConnectorId);
                        }}
                      >
                        目标端任务
                      </Button>
                    ) : (
                      <></>
                    )}
                  </>

                  <>
                    {access.canAdmin ? (
                      <Button
                        type="dashed"
                        ghost
                        onClick={() => {
                          doShowConnectorDetail(record.sourceConnectorId);
                        }}
                      >
                        源端任务
                      </Button>
                    ) : (
                      <></>
                    )}
                  </>

                  <>
                    {access.canAdmin ? (
                      <Button
                        type="dashed"
                        danger
                        ghost
                        key={'delete_' + record.id}
                        onClick={() => {
                          delConnection(record.id);
                        }}
                      >
                        删除链路
                      </Button>
                    ) : (
                      <></>
                    )}
                  </>
                </Space>
              </>,
            ],
          },
        ]}
        headerTitle={false}
        search={false}
        options={false}
        dataSource={expandedDataSource}
        pagination={false}
      />
    );
  };

  return (
    <div>
      <ProTable<API.Connection>
        params={{
          refreshVersion: connectionModel!.refreshVersion!,
        }}
        //rowKey={(record) => String(record.connectionId)}
        rowKey="id"
        // 请求数据API
        request={queryConnection}
        columns={connectionColumns}
        toolbar={{}}
        // 分页设置,默认数据,不展示动态调整分页大小
        // pagination={{
        // 	showSizeChanger: true,
        // 	pageSize: 10
        // }}

        options={{
          setting: {
            listsHeight: 400,
          },
          reload: false,
        }}
        pagination={{
          showQuickJumper: true,
        }}
        search={{ collapsed: false }}
        form={{
          ignoreRules: false,
        }}
        expandedRowRender={(record) => expandedRowRender(record)}
      />

      {/*
				父组件刷新,会导致子组件刷新,所以父组件如果使用state变量,则可以通过变量
				传递给子组件,让子组件更新
				*/}
      <DrawerForm<{
        name: string;
        company: string;
      }>
        title="修改字映射"
        visible={showEditState}
        width={'100%'}
        drawerProps={{
          forceRender: false,
          destroyOnClose: true,
          onClose: () => {
            setShowEditState(false);
          },
        }}
        onFinish={handleUpdateConnection}
      >
        <ConnectionColumnConf
          columnConfProps={{ ...connectionColumnConfPropsState }}
          editorFormRef={editorFormRef}
        />
      </DrawerForm>

      <Drawer
        width={'100%'}
        visible={showDetail}
        onClose={() => {
          setShowDetail(false);
        }}
        closable={true}
      >
        <ConnectionInfo connectionDetail={connectionDetail} />
      </Drawer>

      <Drawer
        width={'100%'}
        visible={showConnectorDetail}
        onClose={() => {
          setShowConnectorDetail(false);
        }}
        closable={true}
      >
        <ConnectorDetail connectorId={connectorId} />
      </Drawer>
    </div>
  );
};

export default ConnectionList;
