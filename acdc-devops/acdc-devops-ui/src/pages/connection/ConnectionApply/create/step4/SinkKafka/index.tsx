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

const TopicList: React.FC = () => {
  const topicColumns: ProColumns<API.DataSystemResource>[] = [
    {
      title: 'topic',
      dataIndex: 'name',
    }
  ];

  const {applyInfoModel, setApplyInfoModel} = useModel('ConnectionApplyModel')
  // 选中
  const [selectId, setSelectId] = useState<number>();
  // 模糊查询
  const [qTopic, setQTopic] = useState<string>();

  const onSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const {value} = e.target;
    setQTopic(value);
  };
  const onSearch = (value: string) => {
    setQTopic(value);
  };

  useEffect(() => {
    if (applyInfoModel.sinkDataCollectionId && applyInfoModel.sinkDataCollectionName) {
      if (applyInfoModel.sinkSearchDataCollection) {
        setQTopic(applyInfoModel.sinkSearchDataCollection);
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
              sinkSearchDataCollection: qTopic
            }

            setApplyInfoModel(newModel)
          },
        };
      }}
      rowClassName={(record) => {
        return record.id === selectId ? styles['split-row-select-active'] : '';
      }}
      params={{
        parentResourceId: applyInfoModel.sinkClusterId,
        resourceTypes: [
          DataSystemResourceTypeConstant.KAFKA_TOPIC
        ],
        name: qTopic,
        //resourceConfigurations: {'name': qTopic}
      }}
      request={pagedQueryDataSystemResource}
      columns={topicColumns}
      toolbar={{
        search: <Search defaultValue={qTopic} value={qTopic} onChange={onSearchChange} onSearch={onSearch} />
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


const ConvertList: React.FC = () => {
  const convertListdataSource: API.KafkaConvertListItem[] = [
    {id: 1, name: 'CDCV1', convert: 'CDC_V1'},
    {id: 2, name: 'JSON', convert: 'JSON'},
    {id: 3, name: 'SCHEMA_LESS_JSON', convert: 'SCHEMA_LESS_JSON'}
  ];
  const convertColumns: ProColumns<API.KafkaConvertListItem>[] = [
    {
      title: '序列化方式',
      dataIndex: 'name',
    }
  ];

  const {applyInfoModel, setApplyInfoModel} = useModel('ConnectionApplyModel')

  // 选中
  const [selectConvert, setSelectConvert] = useState<string>();

  useEffect(() => {
    if (applyInfoModel.sinkKafkaConverterType) {
      setSelectConvert(applyInfoModel.sinkKafkaConverterType);
    }
  }, [applyInfoModel]);
  return (
    <ProTable<API.KafkaConvertListItem>
      onRow={(record) => {
        return {
          onClick: () => {
            setSelectConvert(record.convert);

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
              sinkDataCollectionId: applyInfoModel.sinkDataCollectionId,
              sinkDataCollectionName: applyInfoModel.sinkDataCollectionName,
              sinkSearchDataCollection: applyInfoModel.sinkSearchDataCollection,

              sinkKafkaConverterType: record.convert,
              specificConfiguration: record.convert
            }

            setApplyInfoModel(newModel)
          },
        };
      }}
      rowClassName={(record) => {
        if (record.convert === selectConvert
          && applyInfoModel!.sinkKafkaConverterType! === selectConvert) {
          return styles['split-row-select-active']
        }
        return ''
      }}
      dataSource={convertListdataSource}
      columns={convertColumns}
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
            <TopicList />
          </ProCard>
          <ProCard colSpan="50%" >
            <ConvertList />
          </ProCard>
        </ProCard>
      </RcResizeObserver>
    </ProCard>
  )
};

export default MainPage;
