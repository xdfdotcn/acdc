import React, {useRef, useState} from 'react';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable, {ActionType} from '@ant-design/pro-table';
import {
  disableWideTable, enableWideTable
} from '@/services/a-cdc/api';
import {message, Drawer, Modal} from 'antd';
import {queryWideTable} from '@/services/a-cdc/api';
import {EditOutlined, PoweroffOutlined, PlaySquareOutlined, CopyOutlined, StopOutlined} from '@ant-design/icons';
import WideTableDetail from './components/WideTableDetail';

const {confirm} = Modal;

const WideTableList: React.FC = () => {
  // 详情抽屉控制
  const [showDetail, setShowDetail] = useState<boolean>(false);

  // 编辑抽屉控制
  const [wideTableId, setWideTableId] = useState<number>();

  const [searchWideTableName, setSearchWideTableName] = useState<string>();

  const ref = useRef<ActionType>();

  /**
   更新WideTable 状态
   */
  const enableOrDisableWideTable = (recordWideTableId: number, isEnable: boolean) => {
    confirm({
      title: '确定操作吗?',
      icon: <EditOutlined/>,
      content: isEnable ? '启用 WideTable' : '禁用 WideTable',
      async onOk() {
        if (isEnable) {
          await enableWideTable(recordWideTableId)
        } else {
          await disableWideTable(recordWideTableId);
        }

        ref.current?.reload(false);
        message.success('操作成功');
      },
      onCancel() {
      },
    });
  }

  const requisitionStateValueEnum = {
    APPROVING: {text: '审批中', status: 'APPROVING'},
    REFUSED: {text: '已拒绝', status: 'REFUSED'},
    APPROVED: {text: '审批通过', status: 'APPROVED'},
  };

  const actualStateValueEnum = {
    DISABLED: {text: '未启动', status: 'DISABLED'},
    LOADING: {text: '启动中', status: 'LOADING'},
    READY: {text: '已就绪', status: 'READY'},
    ERROR: {text: '运行错误', status: 'ERROR'},
  };

  const wideTableColumns: ProColumns<API.WideTable>[] = [
    {
      title: '名称',
      width: "10%",
      dataIndex: 'name',
      render: (dom, entity) => {
        return (
          <a
            onClick={() => {
              setWideTableId(entity.id)
              setShowDetail(true)
            }}
          >
            {entity.name}
          </a>
        );
      },
    },
    {
      title: '虚拟表',
      width: "12%",
      dataIndex: 'dataCollectionName',
    },
    {
      title: '描述',
      width: "10%",
      dataIndex: 'description',
    },
    {
      title: '审批情况',
      width: "7%",
      dataIndex: 'requisitionState',
      valueType: 'select',
      valueEnum: requisitionStateValueEnum,
    },
    {
      title: '当前状态',
      width: "7%",
      dataIndex: 'actualState',
      valueType: 'select',
      valueEnum: actualStateValueEnum,
    },
    {
      title: '创建时间',
      width: "15%",
      dataIndex: 'creationTime'
    },
    {
      title: '操作',
      width: "14%",
      valueType: 'option',
      dataIndex: 'option',
      render: (text, record, _, action) => [
        <a
          key={'info_' + record.id}
          onClick={() => {
            setWideTableId(record.id!)
            setShowDetail(true)
          }}
        >
          <CopyOutlined/>
          详情
        </a>,
        <>
          {
            record.requisitionState == 'APPROVED' ? (
              record.desiredState == 'READY' ?
                <a onClick={() => {
                  enableOrDisableWideTable(record.id!, false)
                }}>
                  <PlaySquareOutlined/>
                  禁用
                </a>
                :
                <a onClick={() => {
                  enableOrDisableWideTable(record.id!, true)
                }}>
                  <PoweroffOutlined/>
                  启用
                </a>
            ) : (
              <a style={{cursor: "wait"}}>
                <StopOutlined/>
                {requisitionStateValueEnum[record.requisitionState!].text}
              </a>
            )
          }
        </>
      ],
    },
  ];

  return (
    <div>
      <ProTable<API.WideTable, API.WideTableQuery>
        params={{
          name: searchWideTableName
        }}
        rowKey="id"
        // 请求数据API
        request={queryWideTable}
        columns={wideTableColumns}
        actionRef={ref}
        toolbar={{
          search: {
            onSearch: (value: string) => {
              setSearchWideTableName(value);
            },
          },
        }}
        // 分页设置,默认数据,不展示动态调整分页大小
        pagination={{
          showSizeChanger: false,
          pageSize: 10
        }}
        options={false}
        search={false}
      />

      <Drawer
        width={"100%"}
        visible={showDetail}
        onClose={() => {
          setShowDetail(false)
        }}
        closable={true}
      >
        <WideTableDetail wideTableId={wideTableId!}/>
      </Drawer>
    </div>
  )
};

export default WideTableList;
