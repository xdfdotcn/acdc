import React, {useEffect, useState} from 'react';
import ProCard from '@ant-design/pro-card';
import RcResizeObserver from 'rc-resize-observer';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import styles from './split.less';
import {pagedQueryDataSystemResource} from '@/services/a-cdc/api';
import {useModel} from 'umi';
import {Input} from 'antd';
import {DataSystemResourceTypeConstant} from '@/services/a-cdc/constant/DataSystemResourceTypeConstant';

const {Search} = Input;

const DatabaseList: React.FC = () => {
  const databaseColumns: ProColumns<API.DataSystemResource>[] = [
    {
      title: '数据库名',
      dataIndex: 'name',
    }
  ];

  const {applyInfoModel, setApplyInfoModel} = useModel('ConnectionApplyModel')
  // 选中
  const [selectId, setSelectId] = useState<number>();
  // 模糊查询
  const [queryDatabaseName, setQueryDatabaseName] = useState<string>();

  const onSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const {value} = e.target;
    setQueryDatabaseName(value);
  };
  const onSearch = (value: string) => {
    setQueryDatabaseName(value);
  };

  useEffect(() => {
    if (applyInfoModel.sinkDatabaseId && applyInfoModel.sinkDatabaseName) {
      if (applyInfoModel.sinkSearchDatabase) {
        setQueryDatabaseName(applyInfoModel.sinkSearchDatabase);
      }
      setSelectId(applyInfoModel.sinkDatabaseId);
    }
  }, [applyInfoModel]);

  return (
    <ProTable<API.DataSystemResource, API.DataSystemResourceQuery>
      onRow={(record) => {
        return {
          onClick: () => {
            setSelectId(record.id);

            let newModel = {
              srcPrjId: applyInfoModel.srcPrjId,
              srcPrjName: applyInfoModel.srcPrjName,
              srcSearchPrj: applyInfoModel.srcSearchPrj,
              srcPrjOwnerName: applyInfoModel.srcPrjOwnerName,
              srcClusterId: applyInfoModel.srcClusterId,
              srcDataSystemType: applyInfoModel.srcDataSystemType,
              srcClusterName: applyInfoModel.srcClusterName,
              srcSearchCluster: applyInfoModel.srcSearchCluster,
              srcDatabaseId: applyInfoModel.srcDatabaseId,
              srcDatabaseName: applyInfoModel.srcDatabaseName,
              srcSearchDatabase: applyInfoModel.srcSearchDatabase,
              srcDataCollectionId: applyInfoModel.srcDataCollectionId,
              srcDataCollectionName: applyInfoModel.srcDataCollectionName,
              srcSearchDataCollection: applyInfoModel.srcSearchDataCollection,
              sinkPrjId: applyInfoModel.sinkPrjId,
              sinkPrjName: applyInfoModel.sinkPrjName,
              sinkSearchPrj: applyInfoModel.sinkSearchPrj,
              sinkClusterId: applyInfoModel.sinkClusterId,
              sinkDataSystemType: applyInfoModel.sinkDataSystemType,
              sinkClusterName: applyInfoModel.sinkClusterName,
              sinkSearchCluster: applyInfoModel.sinkSearchCluster,
              sinkInstanceId: applyInfoModel.sinkInstanceId,
              sinkInstanceName: applyInfoModel.sinkInstanceName,
              sinkSearchInstance: applyInfoModel.sinkSearchInstance,

              sinkDatabaseId: record.id,
              sinkDatabaseName: record.name,
              sinkSearchDatabase: queryDatabaseName
            }

            setApplyInfoModel(newModel)
          },
        };
      }}

      rowClassName={(record) => {
        if (record.id === selectId && applyInfoModel!.sinkDatabaseId! === selectId) {
          return styles['split-row-select-active']
        }
        return ''

      }}
      params={{
        parentResourceId: applyInfoModel.sinkClusterId,
        resourceTypes: [
          DataSystemResourceTypeConstant.MYSQL_DATABASE,
          DataSystemResourceTypeConstant.TIDB_DATABASE,
        ],
        name: queryDatabaseName,
        //resourceConfigurations: {'name': queryDatabaseName}
      }}
      request={pagedQueryDataSystemResource}
      columns={databaseColumns}
      toolbar={{
        search: <Search defaultValue={queryDatabaseName} value={queryDatabaseName} onChange={onSearchChange} onSearch={onSearch} />
      }}

      options={false}
      rowKey={(record) => String(record.id)}
      search={false}
      pagination={{
        showSizeChanger: false,
        pageSize: 10
      }}
    />
  )
};


const TableList: React.FC = () => {
  const tableColumns: ProColumns<API.TableListItem>[] = [
    {
      title: '数据表名',
      dataIndex: 'name',
    }
  ];

  const {applyInfoModel, setApplyInfoModel} = useModel('ConnectionApplyModel')
  // 选中
  const [selectId, setSelectId] = useState<number>();
  // 模糊查询
  const [queryTableName, setQueryTableName] = useState<string>();

  const onSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const {value} = e.target;
    setQueryTableName(value);
  };
  const onSearch = (value: string) => {
    setQueryTableName(value);
  };

  useEffect(() => {
    if (applyInfoModel.sinkDataCollectionId && applyInfoModel.sinkDataCollectionName) {
      if (applyInfoModel.sinkSearchDataCollection) {
        setQueryTableName(applyInfoModel.sinkSearchDataCollection);
      }
      setSelectId(applyInfoModel.sinkDataCollectionId);
    }
  }, [applyInfoModel]);

  return (
    <ProTable<API.DataSystemResource, API.DataSystemResourceQuery>
      onRow={(record) => {
        return {
          onClick: () => {
            setSelectId(record.id);
            let newModel = {
              srcPrjId: applyInfoModel.srcPrjId,
              srcPrjName: applyInfoModel.srcPrjName,
              srcSearchPrj: applyInfoModel.srcSearchPrj,
              srcPrjOwnerName: applyInfoModel.srcPrjOwnerName,
              srcClusterId: applyInfoModel.srcClusterId,
              srcDataSystemType: applyInfoModel.srcDataSystemType,
              srcClusterName: applyInfoModel.srcClusterName,
              srcSearchCluster: applyInfoModel.srcSearchCluster,
              srcDatabaseId: applyInfoModel.srcDatabaseId,
              srcDatabaseName: applyInfoModel.srcDatabaseName,
              srcSearchDatabase: applyInfoModel.srcSearchDatabase,
              srcDataCollectionId: applyInfoModel.srcDataCollectionId,
              srcDataCollectionName: applyInfoModel.srcDataCollectionName,
              srcSearchDataCollection: applyInfoModel.srcSearchDataCollection,
              sinkPrjId: applyInfoModel.sinkPrjId,
              sinkPrjName: applyInfoModel.sinkPrjName,
              sinkSearchPrj: applyInfoModel.sinkSearchPrj,
              sinkClusterId: applyInfoModel.sinkClusterId,
              sinkDataSystemType: applyInfoModel.sinkDataSystemType,
              sinkClusterName: applyInfoModel.sinkClusterName,
              sinkSearchCluster: applyInfoModel.sinkSearchCluster,
              sinkInstanceId: applyInfoModel.sinkInstanceId,
              sinkInstanceName: applyInfoModel.sinkInstanceName,
              sinkSearchInstance: applyInfoModel.sinkSearchInstance,
              sinkDatabaseId: applyInfoModel.sinkDatabaseId,
              sinkDatabaseName: applyInfoModel.sinkDatabaseName,
              sinkSearchDatabase: applyInfoModel.sinkSearchDatabase,

              sinkDataCollectionId: record.id,
              sinkDataCollectionName: record.name,
              sinkSearchDataCollection: queryTableName
            }

            setApplyInfoModel(newModel)
          },
        };
      }}
      rowClassName={(record) => {
        if (record.id === selectId && applyInfoModel!.sinkDataCollectionId! === selectId) {
          return styles['split-row-select-active']
        }
        return ''
      }}
      params={{
        parentResourceId: applyInfoModel.sinkDatabaseId,
        name: queryTableName,
        //resourceConfigurations: {'name': queryTableName}
      }}
      request={pagedQueryDataSystemResource}
      columns={tableColumns}
      toolbar={{
        search: <Search defaultValue={queryTableName} value={queryTableName} onChange={onSearchChange} onSearch={onSearch} />
      }}
      options={false}
      rowKey={(record) => String(record.id)}
      search={false}
      pagination={{
        showSizeChanger: false,
        pageSize: 10
      }}
    />
  )
};

// 主页面
const MainPage: React.FC = () => {
  const [responsive, setResponsive] = useState(false);
  return (
    <ProCard
      title="选择数据库"
      bordered
      hoverable
      headerBordered
      style={{
        marginBottom: 16,
        minWidth: 800,
        maxWidth: '100%',
      }}>

      <RcResizeObserver
        key="resize-observer"
        onResize={(offset) => {
          setResponsive(offset.width < 100);
        }}
      >
        <ProCard bordered split={responsive ? 'horizontal' : 'vertical'} >
          <ProCard colSpan="50%">
            <DatabaseList />
          </ProCard>
          <ProCard colSpan="50%" >
            <TableList />
          </ProCard>
        </ProCard>
      </RcResizeObserver>
    </ProCard>
  )
};

export default MainPage;
