import React, { useEffect, useState } from 'react';
import ProCard from '@ant-design/pro-card';
import { Descriptions } from 'antd';
import Field from '@ant-design/pro-field';
import {
  getWideTableConnections,
  getWideTableDetail,
  getWideTableRequisitionBatch,
} from '@/services/a-cdc/api';
import ProTable, { ProColumns } from '@ant-design/pro-table';
import SqColLineageDiagram from '@/components/widetable/SqColLineageDiagram';
import { format } from 'sql-formatter';
import { ProFormField } from '@ant-design/pro-form';

const WideTableDetail: React.FC<{ wideTableId: number }> = ({ wideTableId }) => {
  const [wideTableDetail, setWideTableDetail] = useState<API.WideTableDetail>({});

  const showWideTableDetail = async () => {
    const wideTable = await getWideTableDetail(wideTableId);
    if (!wideTable) {
      return;
    }
    setWideTableDetail(wideTable);
  };

  const getWideTableDetailPage = () => {
    if (!wideTableDetail || !wideTableDetail.id) {
      return <></>;
    }
    return (
      <>
        <ProCard
          title="基础信息"
          headerBordered
          collapsible
          onCollapse={(collapse) => {
            return true;
          }}
        >
          <Descriptions column={2}>
            <Descriptions.Item label="宽表名称">
              <Field text={wideTableDetail?.name} mode="read" />
            </Descriptions.Item>
            <Descriptions.Item label="描述信息">
              <Field text={wideTableDetail?.description} mode="read" />
            </Descriptions.Item>
            <Descriptions.Item label="审批状态">
              <Field text={wideTableDetail?.requisitionState} mode="read" />
            </Descriptions.Item>
            <Descriptions.Item label="当前状态">
              <Field text={wideTableDetail?.actualState} mode="read" />
            </Descriptions.Item>
            <Descriptions.Item label="创建人">
              <Field text={wideTableDetail?.userDomainAccount} mode="read" />
            </Descriptions.Item>
            <Descriptions.Item label="创建时间">
              <Field text={wideTableDetail?.creationTime} mode="read" />
            </Descriptions.Item>
          </Descriptions>
        </ProCard>
        <ProCard title="sql信息" headerBordered collapsible onCollapse={(collapse) => {}}>
          <ProFormField
            ignoreFormItem
            fieldProps={{
              style: {
                width: '100%',
                background: 'black',
              },
            }}
            mode="read"
            valueType="jsonCode"
            text={format(wideTableDetail.selectStatement!, { language: 'mysql' })}
          />
        </ProCard>
      </>
    );
  };
  const [connections, setConnections] = useState([]);

  const [requisitionBatch, setRequisitionBatch] = useState<API.RequisitionBatch[]>([]);

  useEffect(() => {
    showWideTableDetail();
    const fetchDataAndSetState = async () => {
      if (wideTableId != null) {
        const connectionData = await getWideTableConnections(wideTableId);
        setConnections(connectionData);
        const requisitionData = await getWideTableRequisitionBatch(wideTableId);
        setRequisitionBatch(requisitionData);
      }
    };
    fetchDataAndSetState();
  }, [wideTableId]);

  const connectionColumns: ProColumns<API.Connection>[] = [
    {
      title: '源表类型',
      width: '8%',
      dataIndex: 'sourceDataSystemType',
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
      title: '源数据集',
      width: '20%',
      dataIndex: 'sourceDataCollectionName',
      ellipsis: true,
      copyable: true,
    },
    {
      title: '目标数据集',
      width: '30%',
      dataIndex: 'sinkDataCollectionName',
      ellipsis: true,
      copyable: true,
    },
    {
      title: '审批状态',
      width: '6%',
      dataIndex: 'requisitionState',
      valueType: 'select',
      valueEnum: {
        APPROVING: { text: '审批中', status: 'APPROVING' },
        REFUSED: { text: '已拒绝', status: 'REFUSED' },
        APPROVED: { text: '审批通过', status: 'APPROVED' },
      },
    },
    {
      title: '运行状态',
      width: '6%',
      dataIndex: 'actualState',
      valueType: 'select',
      valueEnum: {
        STARTING: { text: '启动中', status: 'STARTING' },
        RUNNING: { text: '运行中', status: 'RUNNING' },
        STOPPING: { text: '停止中', status: 'STOPPING' },
        STOPPED: { text: '停止', status: 'STOPPED' },
        FAILED: { text: '运行失败', status: 'FAILED' },
      },
    },
    {
      title: '创建时间',
      width: '13%',
      dataIndex: 'creationTime',
      search: false,
    },
  ];

  const getWideTableConnectionList = () => {
    if (!wideTableDetail || !wideTableDetail.id) {
      return <></>;
    }

    return (
      <ProTable
        columns={connectionColumns}
        dataSource={connections}
        pagination={false} // 禁用分页
        search={false} // 禁用搜索
      />
    );
  };

  const requisitionBatchColumns: ProColumns<API.RequisitionBatch>[] = [
    {
      title: '状态',
      width: '20%',
      dataIndex: 'state',
      ellipsis: true,
      onFilter: true,
      valueType: 'select',
      valueEnum: {
        PENDING: { text: '初始化', status: 'Success' },
        APPROVING: { text: '审批中', status: 'Success' },
        APPROVED: { text: '审批通过', status: 'Success' },
        REFUSED: { text: '已拒绝', status: 'Success' },
      },
    },
    {
      title: '申请人',
      width: '20%',
      dataIndex: 'domainAccount',
      ellipsis: true,
      copyable: true,
    },
    {
      title: '创建时间',
      width: '30%',
      dataIndex: 'creationTime',
      ellipsis: true,
      copyable: true,
    },
  ];

  const requisitionColumns: ProColumns<API.Requisition>[] = [
    {
      title: '状态',
      width: '7%',
      dataIndex: 'state',
      ellipsis: true,
      onFilter: true,
      valueType: 'select',
      valueEnum: {
        APPROVING: { text: '审批中', status: 'Success' },
        SOURCE_OWNER_APPROVING: { text: '源端负责人审批中', status: 'Success' },
        SOURCE_OWNER_REFUSED: { text: '源端负责人已拒绝', status: 'Success' },
        DBA_APPROVING: { text: 'DBA审批中', status: 'Success' },
        DBA_REFUSED: { text: 'DBA已拒绝', status: 'Success' },
        APPROVED: { text: '审批通过', status: 'Success' },
      },
    },
    {
      title: 'OA系统ID',
      width: '10%',
      dataIndex: 'thirdPartyId',
      ellipsis: true,
      copyable: true,
    },
    {
      title: '申请原因',
      width: '10%',
      dataIndex: 'description',
      ellipsis: true,
      copyable: true,
    },
    {
      title: '申请人',
      width: '7%',
      dataIndex: 'userDomainAccount',
      ellipsis: true,
      copyable: true,
    },
    {
      title: '源端项目名',
      width: '10%',
      dataIndex: 'sourceProjectName',
      ellipsis: true,
      copyable: true,
    },
    {
      title: '源端审批人',
      width: '5%',
      dataIndex: 'sourceApproverDomainAccount',
      ellipsis: true,
      copyable: true,
    },
    {
      title: '源端审批意见',
      width: '5%',
      dataIndex: 'sourceApprovalComments',
      ellipsis: true,
      copyable: true,
    },
    {
      title: 'dba审批人',
      width: '5%',
      dataIndex: 'dbaApproverDomainAccount',
      ellipsis: true,
      copyable: true,
    },
    {
      title: 'dba审批意见',
      width: '5%',
      dataIndex: 'dbaApprovalComments',
      ellipsis: true,
      copyable: true,
    },
  ];

  const getRequisition = () => {
    if (!wideTableDetail || !wideTableDetail.id) {
      return <></>;
    }

    return (
      <>
        <ProCard title="审批总览" headerBordered collapsible onCollapse={(collapse) => {}}>
          <ProTable
            columns={requisitionBatchColumns}
            dataSource={requisitionBatch}
            pagination={false} // 禁用分页
            search={false} // 禁用搜索
          />
        </ProCard>
        <ProCard title="审批单详情" headerBordered collapsible onCollapse={(collapse) => {}}>
          <ProTable
            columns={requisitionColumns}
            dataSource={requisitionBatch[0]?.requisitions}
            pagination={false} // 禁用分页
            search={false} // 禁用搜索
          />
        </ProCard>
      </>
    );
  };
  return (
    <ProCard
      tabs={{
        type: 'card',
      }}
    >
      <ProCard.TabPane key="tab1" tab="宽表详情">
        {getWideTableDetailPage()}
      </ProCard.TabPane>

      <ProCard.TabPane key="tab2" tab="宽表血缘">
        <ProCard title="血缘关系展示" headerBordered collapsible style={{ height: '75vh' }}>
          <SqColLineageDiagram subQuery={wideTableDetail.subQuery} />
        </ProCard>
      </ProCard.TabPane>

      <ProCard.TabPane key="tab3" tab="审批记录">
        {getRequisition()}
      </ProCard.TabPane>

      <ProCard.TabPane key="tab4" tab="关联链路">
        <ProCard title="关联链路列表">{getWideTableConnectionList()}</ProCard>
      </ProCard.TabPane>
    </ProCard>
  );
};

export default WideTableDetail;
