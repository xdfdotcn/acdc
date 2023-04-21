import React, {useEffect, useState} from 'react';
import ProCard from '@ant-design/pro-card';
import RcResizeObserver from 'rc-resize-observer';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import styles from './index.less';
import {pagedQueryDataSystemResource, pagedQueryProject} from '@/services/a-cdc/api';
import {useModel} from 'umi';
import {Input} from 'antd';
import {ProjectConstant} from '@/services/a-cdc/constant/ProjectConstant';
import {DataSystemTypeConstant} from '@/services/a-cdc/constant/DataSystemTypeConstant';
import {DataSystemResourceTypeConstant} from '@/services/a-cdc/constant/DataSystemResourceTypeConstant';

const {Search} = Input;

const ProjectList: React.FC = () => {
  const {applyInfoModel, setApplyInfoModel} = useModel('ConnectionApplyModel')
  const projectColumns: ProColumns<API.Project>[] = [
    {
      title: '项目名称',
      dataIndex: 'name',
    },
    {
      title: '项目描述',
      dataIndex: 'description',
      ellipsis: true,
    },
  ];

  // 选中
  const [selectId, setSelectId] = useState<number>();
  // 模糊查询
  const [queryProjectName, setQueryProjectName] = useState<string>();

  const onSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const {value} = e.target;
    setQueryProjectName(value);
  };
  const onSearch = (value: string) => {
    setQueryProjectName(value);
  }

  useEffect(() => {
    if (applyInfoModel.sinkPrjId && applyInfoModel.sinkPrjName) {
      if (applyInfoModel.sinkSearchPrj) {
        setQueryProjectName(applyInfoModel.sinkSearchPrj);
      }
      setSelectId(applyInfoModel.sinkPrjId);
    }
  }, [applyInfoModel]);

  return (
    <ProTable<API.Project, API.ProjectQuery>
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

              sinkPrjId: record.id,
              sinkPrjName: record.name,
              sinkSearchPrj: queryProjectName
            }

            setApplyInfoModel(newModel)
          },
        };
      }}

      rowClassName={(record) => {
        if (record.id === selectId && applyInfoModel!.sinkPrjId! === selectId) {
          return styles['split-row-select-active']
        }
        return ''
      }}
      params={{
        name: queryProjectName,
        queryRange: ProjectConstant.QUERY_RANGE_CURRENT_USER
      }}
      // 请求数据API
      request={pagedQueryProject}
      columns={projectColumns}
      toolbar={{
        search: <Search defaultValue={queryProjectName} value={queryProjectName} onChange={onSearchChange} onSearch={onSearch} />
      }}
      // 分页设置,默认数据,不展示动态调整分页大小
      pagination={{
        showSizeChanger: false,
        pageSize: 10
      }}
      options={false}
      rowKey={(record) => String(record.id)}
      search={false}
    />
  )
};

// rdb列表,处理联动
const ClusterList: React.FC = () => {
  const clusterColumns: ProColumns<API.DataSystemResource>[] = [
    {
      title: '集群',
      dataIndex: 'name',
    },
    {
      title: '类型',
      dataIndex: 'resourceType'
    },
  ];

  const {applyInfoModel, setApplyInfoModel} = useModel('ConnectionApplyModel')
  // 选中
  const [selectId, setSelectId] = useState<number>();
  // 模糊查询
  const [queryClusterName, setQueryClusterName] = useState<string>();

  const onSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const {value} = e.target;
    setQueryClusterName(value);
  };
  const onSearch = (value: string) => {
    setQueryClusterName(value);
  };

  useEffect(() => {
    if (applyInfoModel.sinkClusterId && applyInfoModel.sinkClusterName) {
      if (applyInfoModel.sinkSearchCluster) {
        setQueryClusterName(applyInfoModel.sinkSearchCluster);
      }

      setSelectId(applyInfoModel.sinkClusterId);
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

              sinkClusterId: record.id,
              sinkDataSystemType: record.dataSystemType,
              sinkClusterName: record.name,
              sinkSearchCluster: queryClusterName
            }

            setApplyInfoModel(newModel)
          },
        };
      }}
      rowClassName={(record) => {
        if (record.id === selectId && applyInfoModel!.sinkClusterId! === selectId) {
          return styles['split-row-select-active']
        }
        return ''

      }}
      params={{
        projectId: applyInfoModel.sinkPrjId,
        name: queryClusterName,
        resourceTypes: [
          DataSystemResourceTypeConstant.MYSQL_CLUSTER,
          DataSystemResourceTypeConstant.TIDB_CLUSTER,
          DataSystemResourceTypeConstant.KAFKA_CLUSTER,
          DataSystemResourceTypeConstant.HIVE,
        ],
        //resourceConfigurations: {'name': queryClusterName}
      }}
      request={pagedQueryDataSystemResource}
      columns={clusterColumns}
      toolbar={{
        search: <Search defaultValue={queryClusterName} value={queryClusterName} onChange={onSearchChange} onSearch={onSearch} />
      }}
      pagination={{
        showSizeChanger: false,
        pageSize: 100
      }}
      options={false}
      rowKey={(record) => String(record.id)}
      search={false}
    />
  )
};

//sink instance
const InstList: React.FC = () => {
  const instColumns: ProColumns<API.DataSystemResource>[] = [
    {
      title: '实例名称',
      dataIndex: 'name',
      width: '80%'
    }
  ];

  const {applyInfoModel, setApplyInfoModel} = useModel('ConnectionApplyModel')
  const [selectId, setSelectId] = useState<number>();
  const [queryInstName, setQueryInstName] = useState<string>();

  const onSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const {value} = e.target;
    setQueryInstName(value);
  };

  const onSearch = (value: string) => {
    setQueryInstName(value);
  };

  useEffect(() => {
    if (applyInfoModel.sinkInstanceId && applyInfoModel.sinkInstanceName) {
      if (applyInfoModel.sinkSearchInstance) {
        setQueryInstName(applyInfoModel.sinkSearchInstance);
      }
      setSelectId(applyInfoModel.sinkInstanceId);
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

              sinkInstanceId: record.id,
              sinkInstanceName: record.name,
              sinkSearchInstance: queryInstName
            }

            setApplyInfoModel(newModel)
          },
        };
      }}
      rowClassName={(record) => {
        if (record.id === selectId && applyInfoModel!.sinkInstanceId! === selectId) {
          return styles['split-row-select-active']
        }
        return ''
      }}

      params={{
        parentResourceId: applyInfoModel.sinkClusterId,
        resourceTypes: [
          DataSystemResourceTypeConstant.MYSQL_INSTANCE,
          DataSystemResourceTypeConstant.TIDB_SERVER
        ],
        name: queryInstName,
        //resourceConfigurations: {'name': queryInstName}
      }}

      request={pagedQueryDataSystemResource}
      columns={instColumns}
      toolbar={{
        search: <Search defaultValue={queryInstName} value={queryInstName} onChange={onSearchChange} onSearch={onSearch} />
      }}
      pagination={{
        showSizeChanger: false,
        pageSize: 10
      }}
      options={false}
      rowKey={(record) => String(record.id)}
      search={false}
    />
  )
};

const Step3: React.FC = () => {
  const [responsive, setResponsive] = useState(false);
  const {applyInfoModel} = useModel('ConnectionApplyModel')
  return (
    // main page
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
          <ProCard>
            <ProjectList />
          </ProCard>
          <ProCard >
            <ClusterList />
          </ProCard>
            <ProCard ><InstList /></ProCard>
        </ProCard>
      </RcResizeObserver>
    </ProCard>
  )
};

export default Step3;
