import React, { useState } from "react";
import {Table, Input, Button, Popconfirm, message, Drawer, Space, Spin} from 'antd';
import styles from './index.less';
import ConnectorApply from './create';
import { applyMultiConnector } from '@/services/a-cdc/api';
import { history, useModel } from 'umi';
import ConfigForm from './config';


const { TextArea } = Input;

const ConnectionApply: React.FC<{}> = () =>{
  // {origin:333, purpose: 555, id: 4}
  const {applyInfoModel,setApplyInfoModel} = useModel('ConnectorApplyModel');
  const { fieldMappingModel, setFieldMappingModel } = useModel('FieldMappingModel');
  const [loading, setLoading] = useState(false);
  const [dataSource, setDataSource] = useState<any[]>([]);
  const [drawerVisible, setDrawerVisible] = useState(false);
  const [value, setValue] = useState('');
  const [idx, setidx] = useState(1);
  const [step, setStep] = useState(0);
  const [configModalVisible, handleModalVisible] = useState(false);
  const [selectItem, setSelectItem] = useState<any>({});
  const [selectKey, setSelectKey] = useState<any>({});
  const onDrawerClose = () => {
    sessionStorage.removeItem('applyInfo');
    setDrawerVisible(false);
    setStep(0);
  };
  const handleAdd = () => {
    // sessionStorage.setItem('applyInfo',JSON.stringify({
    //   id: 0,
    //   origin:'数据通道/CDC-dataline/debezium_source/city',
    //   purpose: '数据通道/CDC-dataline/172.24.142.118:3306/debezium_sink/sink_city',
    //   //src project
    //   srcPrjId: 1803,
    //   srcPrjName: "数据通道",
    //   srcSearchPrj: "数据",
    //   //src cluster
    //   srcClusterId: 161,
    //   srcClusterName: "CDC-dataline",
    //   srcDataSystemType: "mysql",
    //   // srcSearchCluster: "de",
    //   //src database
    //   srcDatabaseId: 161,
    //   srcDatabaseName: "debezium_source",
    //   srcSearchDatabase: "de",
    //   //src dataset
    //   srcDataSetId: 6817,
    //   srcDataSetName: "city",
    //   srcSearchDataset: '',
    //   //sink project
    //   sinkPrjId: 1803,
    //   sinkPrjName: "数据通道",
    //   sinkSearchPrj: '数据',
    //   //sink cluster
    //   sinkClusterId: 161,
    //   sinkClusterName: "CDC-dataline",
    //   sinkClusterType: "RDB",
    //   sinkSearchCluster: 'd',
    //   //sink instance
    //   sinkInstanceId: 1926,
    //   sinkInstanceName: "172.24.142.118:3306",
    //   sinkSearchInstance: '',
    //   //sink database
    //   sinkDatabaseId: 160,
    //   sinkDatabaseName: "debezium_sink",
    //   sinkSearchDatabase: 'de',
    //   // sink dataset
    //   sinkDataSetId: 6816,
    //   sinkDataSetName: "sink_city",
    //   sinkDataSystemType: "mysql",
    //   sinkSearchDataSet: ''
    // }))
    if(sessionStorage.getItem('applyInfo')){
      setApplyInfoModel(JSON.parse(sessionStorage.getItem('applyInfo')))
    }
    setStep(0);
    setDrawerVisible(true);
  };
  const handleDelete = (record) => {
    setDataSource(dataSource.filter((item) => item.id !== record.id));
  };
  const defaultColumns = [
    {
      title: '数据源表',
      dataIndex: 'origin',
    },
    {
      title: '数据目标表',
      dataIndex: 'purpose',
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
            handleModalVisible(true);
          }}
        >配置</Button>
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
                async() => {
                  setLoading(true)
                  const res = await applyMultiConnector({
                    connections: dataSource,
                    description: value
                  });
                  message.success('提交成功');
                  history.push("/connection/ConnectionMgt")
                }
              }
            >提交</Button>
            <Button
              style={{ marginTop: 16 }}
              onClick={() => {
                history.push("/connection/ConnectionMgt")
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
          <ConnectorApply
            currentStep={step}
            onSubmit={(res)=>{
              setDataSource(dataSource.concat([{
                ...res,
                id: idx,
                origin: `${res.srcPrjName}/${res.srcClusterName}/${res.srcDatabaseName}/${res.srcDataSetName}`,//项目/集群/数据库/数据表
                purpose: `${res.sinkPrjName}/${res.sinkClusterName}/${res.sinkInstanceName}/${res.sinkDatabaseName}/${res.sinkDataSetName}`,//项目/集群/数据实例/数据库/数据表'
              }]));
              setidx(idx + 1);
              onDrawerClose()
            }}
          />
        </Drawer>

        <ConfigForm
          selectItem={selectItem}
          onCancel={() => handleModalVisible(false)}
          modalVisible={configModalVisible}
          onSubmit={ (res)=>{
            const copy = dataSource;
            dataSource[selectKey] = res;
            setDataSource(copy)
            handleModalVisible(false);
            setSelectKey('');
            setSelectItem({});
          }}
        />
      </div>
    </Spin>
  );
}

export default ConnectionApply;
