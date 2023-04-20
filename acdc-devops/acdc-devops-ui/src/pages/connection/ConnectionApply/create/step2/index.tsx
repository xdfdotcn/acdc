import React, {useEffect, useState} from 'react';
import ProCard from '@ant-design/pro-card';
import RcResizeObserver from 'rc-resize-observer';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import styles from './index.less';
import {pagedQueryDataSystemResource} from '@/services/a-cdc/api';
import {Input} from 'antd';
import {useModel} from 'umi';
import {DataSystemResourceTypeConstant} from '@/services/a-cdc/constant/DataSystemResourceTypeConstant';

const {Search} = Input;

const DatabaseList: React.FC = () => {
  const {applyInfoModel, setApplyInfoModel} = useModel('ConnectionApplyModel')
  const databaseColumns: ProColumns<API.DataSystemResource>[] = [
    {
      title: '数据库名',
      dataIndex: 'name',
    }
  ];

  // 选中
  const [selectId, setSelectId] = useState<number>();
  // 模糊搜索
  const [queryDatabaseName, setQueryDatabaseName] = useState<string>();
  const onSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const {value} = e.target;
    setQueryDatabaseName(value);
  };

  useEffect(() => {
    if (applyInfoModel.srcDatabaseName && applyInfoModel.srcDatabaseId) {
      if (applyInfoModel.srcSearchDatabase) {
        setQueryDatabaseName(applyInfoModel.srcSearchDatabase);
      }
      setSelectId(applyInfoModel.srcDatabaseId);
    }
  }, [applyInfoModel])
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

              srcDatabaseId: record.id,
              srcDatabaseName: record.name,
              srcSearchDatabase: queryDatabaseName
            }
            setApplyInfoModel(newModel)
          },
        };
      }}

      rowClassName={(record) => {
        if (record.id === selectId && applyInfoModel!.srcDatabaseId! === selectId) {
          return styles['split-row-select-active']
        }
        return ''
      }}

      params={{
        parentResourceId: applyInfoModel.srcClusterId,
        resourceTypes: [
          DataSystemResourceTypeConstant.MYSQL_DATABASE,
          DataSystemResourceTypeConstant.TIDB_DATABASE
        ],
        name: queryDatabaseName,
        //resourceConfigurations: {'name': queryDatabaseName}
      }}

      request={pagedQueryDataSystemResource}

      columns={databaseColumns}

      toolbar={{
        search: <Search defaultValue={queryDatabaseName} value={queryDatabaseName} onChange={onSearchChange} />
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
  const tableColumns: ProColumns<API.DataSystemResource>[] = [
    {
      title: '数据表名',
      dataIndex: 'name',
    }
  ];

  const {applyInfoModel, setApplyInfoModel} = useModel('ConnectionApplyModel')
  const [selectId, setSelectId] = useState<number>();
  const [queryTableName, setQueryTableName] = useState<string>();

  const onSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const {value} = e.target;
    setQueryTableName(value);
  };
  const onSearch = (value: string) => {
    setQueryTableName(value);
  };

  useEffect(() => {
    if (applyInfoModel.srcDataCollectionId && applyInfoModel.srcDataCollectionName) {
      if (applyInfoModel.srcSearchDataCollection) {
        setQueryTableName(applyInfoModel.srcSearchDataCollection);
      }
      setSelectId(applyInfoModel.srcDataCollectionId);
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

              srcDataCollectionId: record.id,
              srcDataCollectionName: record.name,
              srcSearchDataCollection: queryTableName
            }

            setApplyInfoModel(newModel)
          },
        };
      }}
      rowClassName={(record) => {
        if (record.id === selectId && applyInfoModel!.srcDataCollectionId! === selectId) {
          return styles['split-row-select-active']
        }
        return ''
      }}
      params={{
        parentResourceId: applyInfoModel.srcDatabaseId,
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

const Step2: React.FC = () => {
  const [responsive, setResponsive] = useState(false);
  // 联动查询,父组件刷新触发子组件刷新
  return (
    <ProCard
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

export default Step2;
