/* eslint-disable react/prop-types */
import React, {useRef, useState} from 'react';
import {Breadcrumb, Button, Card, ConfigProvider, Drawer, message, Modal, Select, Space, Tag} from 'antd';
import ProTable, {EditableFormInstance, ProColumns} from '@ant-design/pro-table';
import {history} from 'umi';
import ProForm, {DrawerForm, ModalForm, ProFormInstance, ProFormTextArea, StepsForm,} from '@ant-design/pro-form';
import DcSearcher, {ProjectNode, SearchRecord, SearchScope,} from '../connection/components/DcSearcher';
import {DataSystemTypeConstant} from '@/services/a-cdc/constant/DataSystemTypeConstant';
import ConnectionColumnConf, {ConnectionColumnConfProps,} from '../connection/components/ConnectionColumnConf';
import {generateConnectionColumnConf, getDataCollectionDefinition, pagedQueryDataSystemResource, validateDataCollection,} from '@/services/a-cdc/api';
import {verifyUKWithShowMessage} from '@/services/a-cdc/connection/connection-column-conf-service';
import {EditOutlined} from '@ant-design/icons';
import {DataSystemResourceTypeConstant} from '@/services/a-cdc/constant/DataSystemResourceTypeConstant';

const { Option } = Select;

const { confirm } = Modal;

// 滚动条动态计算
const S_SCROLL = {
  y: 'calc(100vh - 260px)',
  scrollToFirstRowOnChange: true,
};

const SOURCE = 'source';

const SINK = 'sink';

// 行记录开始 rowkey 索引，解决rowKey为数值类型的情况，"1","0"特殊情况导致的bug
const BEGIN_ROW_INDEX: number = 1024;

// 面包屑 标签 row key 前缀
const B_ROW_KEY_PREFIX: string = 'b_';

// tag 标签 row key 前缀
const T_ROW_KEY_PREFIX: string = 't_';

/**
 * 链路申请记录
 */
type ConnectionReqRecord = {
  // key
  key: number;

  // source 搜索记录
  source: SearchRecord;

  // sink 搜索记录
  sink: SearchRecord;

  // 强制设定的 source 端口项目
  srcForceSetProject: ProjectNode;

  // 强制设定的 sink 端项目
  sinkForceSetProject: ProjectNode;

  // 特殊配置
  specificConf?: string;

  // 字段列原始配置
  originClConf: API.ConnectionColumnConf[];

  // 字段列当前配置
  currentClConf: API.ConnectionColumnConf[];

  // 目标端实例，目前只有TIDB存在
  sinkInstanceId?: number;
};

const ConnctoinRequisition: React.FC = () => {
  // 添加链路窗口显示控制
  const [addConnDwOpenSt, setAddConnDwOpenSt] = useState<boolean>(false);

  // 编辑链路窗口显示控制
  const [editConnDwOpenSt, setEditConnDwOpenSt] = useState<boolean>(false);

  // 提交表单窗口显示控制
  const [reqReasonDwOpenSt, setReqReasonDwOpenSt] = useState<boolean>(false);

  // 源端，目标端选择的搜索记录
  const [sourceSrListSt, setSourceSrListSt] = useState<SearchRecord[]>([]);
  const [sinkSrListSt, setSinkSrListSt] = useState<SearchRecord[]>([]);

  // 目前只支持分库分表的情况批量创建，可以选择多个source，请求字段映射默认使用
  // source集合中的第一个
  const [srcFirstSrSt, setSrcFirstSrSt] = useState<SearchRecord>();
  const [sinkFirstSrSt, setSinkFirstSrSt] = useState<SearchRecord>();

  // 字段映射配置
  const [connClConfWhenAddSt, setConnClConfWhenAddSt] = useState<ConnectionColumnConfProps>();
  const [connClConfWhenEditSt, setConnClConfWhenEditSt] = useState<ConnectionColumnConfProps>();
  const connClConfWhenAddRef = useRef<EditableFormInstance<API.ConnectionColumnConf>>();
  const connClConfWhenEditRef = useRef<EditableFormInstance<API.ConnectionColumnConf>>();

  // 链路配置相关状态
  const [srcForceSetPrjListSt, setSrcForceSetPrjListSt] = useState<ProjectNode[]>([]);

  // 源端可能存在多个项目的情况，需要用户强制指定
  const [srcForceSetPrjSt, setSrcForceSetPrjSt] = useState<ProjectNode>();

  // 如果 sink 为 kafka 类型，则需要选择序列化类型
  const [kafkaDataFormatSt, setKafkaDataFormatSt] = useState<string>();

  const [kafkaDataFormatHiddenSt, setKafkaDataFormatHiddenSt] = useState<boolean>(true);

  const [connReqRecordListSt, setConnReqRecordListSt] = useState<ConnectionReqRecord[]>([]);

  // 编辑当前申请记录
  const [editConnReqRecordSt, setEditConnReqRecordSt] = useState<ConnectionReqRecord>();

  // 加载 version
  const [loadVSt, setLoadVSt] = useState<number>(-99);

  const [refreshVSt, setRefreshVSt] = useState<number>(-99);

  // 分步表单当前步骤
  const [stepCurrentSt, setStepCurrentSt] = useState<number>(0);

  // 滚动显示动态控制，解决没有数据的时候，显示滚动条的问题
  const [scrollSt, setScrollSt] = useState({});

  // 申请单提交申请理由窗口form
  const reqReasonDwRef = useRef<
    ProFormInstance<{
      description?: string;
    }>
  >();
  // 提交申请按钮状态控制，防止重复提交
  const [reqSubmitBtnSt, setReqSubmitBtnSt] = useState<boolean>(false);

  /**
   * 数据列表为空自定义展示.
   */
  const customizeRenderEmpty = () => (
    <div style={{ textAlign: 'left' }}>
      <p style={{ fontSize: 15 }}>申请步骤</p>
      <StepsForm
        containerStyle={{ width: '100%', height: '100%' }}
        current={-1}
        stepsProps={{ direction: 'vertical' }}
        submitter={{ render: () => [] }}
      >
        <StepsForm.StepForm title="选择源端数据集"></StepsForm.StepForm>
        <StepsForm.StepForm title="选择目标端数据集"></StepsForm.StepForm>
        <StepsForm.StepForm title="配置字段映射"></StepsForm.StepForm>
        <StepsForm.StepForm title="选择项目负责人"></StepsForm.StepForm>
        <StepsForm.StepForm title="输入申请理由"></StepsForm.StepForm>
        <StepsForm.StepForm title="提交审批,等待审批通过"></StepsForm.StepForm>
        <StepsForm.StepForm title="进入链路管理页,启动链路"></StepsForm.StepForm>
      </StepsForm>
      <br></br>
      <p style={{fontSize: 15}}>源端支持的数据系统</p>
      <Tag>MySQL</Tag>
      <Tag>TiDB</Tag>
      <br></br>
      <br></br>
      <br></br>
      <p style={{fontSize: 15}}>目标端支持的数据系统</p>
      <Tag>MySQL</Tag>
      <Tag>TiDB</Tag>
      <Tag>SQLServer</Tag>
      <Tag>Oracle</Tag>
      <Tag>Hive</Tag>
      <Tag>Starrocks</Tag>
      <Tag>Elasticsearch</Tag>
      <Tag>Kafka</Tag>
      <br></br>
      <br></br>
      <br></br>
    </div>
  );

  /**
   * Max Row key.
   **/
  const beginRk = () => {
    if (connReqRecordListSt.length == 0) {
      return BEGIN_ROW_INDEX;
    }
    return connReqRecordListSt[connReqRecordListSt.length - 1].key + 1;
  };

  const uK = (source: SearchRecord, sink: SearchRecord) => {
    return source.resourceNode?.id + '_' + sink.resourceNode?.id;
  };

  /**
   * 添加链路申请记录窗口,关闭事件监听.
   */
  const addConnDwOnClose = () => {
    setSinkSrListSt([]);
    setSinkFirstSrSt(undefined);
    setAddConnDwOpenSt(false);
  };

  /**
   * 编辑链路申请记录窗口，关闭事件监听
   */
  const editConnDwOnClose = () => {
    setEditConnDwOpenSt(false);
  };

  /**
   * 链路申请记录编辑，提交事件监听
   */
  const editConnDwOnSubmit = async (record?: ConnectionReqRecord) => {
    if (!record) return false;

    const { source, sink } = record;
    const recordUk = uK(source, sink);
    const connReqRecordList: ConnectionReqRecord[] = [];
    for (let r of connReqRecordListSt) {
      connReqRecordList.push(r);
    }

    for (let it of connReqRecordList) {
      const itSrcNode = it.source;
      const itSinkNode = it.sink;
      const itUk = uK(itSrcNode, itSinkNode);
      if (itUk == recordUk) {
        let clConf = connClConfWhenEditRef.current?.getRowsData?.();
        // check
        if (
          !(await checkTableModelAndUk(
            itSinkNode.resourceNode!.id!,
            clConf!,
            itSinkNode.resourceNode!.dataSystemType,
          ))
        ) {
          return false;
        }

        it.currentClConf = clConf!;
      }
    }

    loadRecords(connReqRecordList);
    editConnDwOnClose();
    return true;
  };

  /**
   * 链路申请记录编辑事件监听
   */
  const editConnOnClick = (record: ConnectionReqRecord) => {
    // 2. 更新columnConf组件数据
    const { source, sink } = record;
    const { resourceNode: sourceDc } = source;
    const { resourceNode: sinkDc } = sink;
    setConnClConfWhenEditSt({
      displayDataSource: record.currentClConf,
      originalDataSource: record.originClConf,
      canEdit: sinkDc!.dataSystemType != DataSystemTypeConstant.KAFKA,
      canDelete: sinkDc!.dataSystemType == DataSystemTypeConstant.KAFKA,
      sinkDataSystemType: sinkDc!.dataSystemType,
      sinkDataCollectionId: sinkDc!.id,
      sourceDataCollectionId: sourceDc!.id,
    });

    setEditConnReqRecordSt(record);
    setEditConnDwOpenSt(true);
  };

  /**
   * 申请记录删除事件监听.
   **/
  const delConnOnClick = (record: ConnectionReqRecord) => {
    confirm({
      title: '确定删除吗',
      icon: <EditOutlined />,
      content: '删除本条申请记录',
      async onOk() {
        const connReqRecordList: ConnectionReqRecord[] = [];
        for (let r of connReqRecordListSt) {
          connReqRecordList.push(r);
        }

        const { source, sink } = record;
        const recordUk = uK(source, sink);

        let newConnReqRecordList = connReqRecordList.filter((it) => {
          let itUk = uK(it.source, it.sink);
          return itUk !== recordUk;
        });

        loadRecords(newConnReqRecordList);
      },
      onCancel() {},
    });
  };

  /**
   * 提交链路申请表单事件监听.
   */
  const connReqSubmitBtnOnClick = () => {
    if (connReqRecordListSt.length == 0) {
      message.warn('请添加链路');
      return;
    }

    setReqReasonDwOpenSt(true);
  };

  const manageLinkBtnOnClick = () => {
    history.push('/connection/connection-mgt');
  };

  /**
   * 提交申链路申请单.
   **/
  const reqReasonDwOnSubmit = async () => {
    // 表单校验
    await reqReasonDwRef.current?.validateFields();

    const formObj = reqReasonDwRef.current?.getFieldsFormatValue?.();

    const description = formObj?.description;

    // 转换为接口请求数据结构
    let connections: API.ConnectionDetail[] = [];
    connReqRecordListSt.forEach((element, _index, _arr) => {
      let connectionColumnConfigurations: API.ConnectionColumnConf[] = [];
      let columnConfDisplayData = element.currentClConf as API.ConnectionColumnConf[];
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
        });
      });

      const { source, sink } = element;
      const { resourceNode: srcNode } = source;
      const { resourceNode: sinkNode } = sink;

      connections.push({
        sourceDataSystemType: srcNode!.dataSystemType!,
        sourceProjectId: element.srcForceSetProject.id!,
        sourceDataCollectionId: srcNode!.id!,
        sinkDataCollectionId: sinkNode!.id!,
        sinkDataSystemType: sinkNode!.dataSystemType!,
        sinkProjectId: element.sinkForceSetProject.id!,
        specificConfiguration: element.specificConf!,
        sinkInstanceId: element.sinkInstanceId,
        connectionColumnConfigurations: connectionColumnConfigurations!,
      });
    });

    let reqBody: API.ConnectionRequisitionDetail = {
      connections: connections,
      description: description,
    };

    // 提交申请接口请求

    confirm({
      title: '确定提交链路申请单吗?',
      icon: <EditOutlined />,
      content: '链路申请单',
      async onOk() {
        reqReasonDwOnCancel();
        setReqSubmitBtnSt(true);
        // 自定义 http response 处理
        // issues/935   https://github.com/ant-design/ant-design-pro/issues/935
        fetch(
          new Request('/api/v1/connections', {
            method: 'post',
            headers: {
              'Content-Type': 'application/json;charset=utf-8',
            },
            body: JSON.stringify(reqBody),
          }),
        ).then((response) => {
          // promise

          if (response.status == 200) {
            setReqSubmitBtnSt(false);
            message.success('提交成功');
            history.push('/connection/connection-mgt');
            return;
          }

          if (response.status == 409) {
            response.json().then((jsonBody) => {
              setReqSubmitBtnSt(false);
              message.warn(jsonBody.errorMessage);
            });
          } else {
            response.json().then((jsonBody) => {
              message.error(jsonBody.errorMessage);
            });
          }
        });
      },
      onCancel() {},
    });
  };
  const reqReasonDwOnCancel = async () => {
    setReqReasonDwOpenSt(false);
  };

  /**
   * 添加链路，步骤1.
   */
  const onStep1 = async () => {
    if (!sourceSrListSt || sourceSrListSt.length <= 0) {
      message.warn('请选择源端数据集');
      return false;
    }

    // 校验源端数据集是否相同
    const ids: number[] = [];
    for (let record of sourceSrListSt) {
      const { resourceNode } = record;
      ids.push(resourceNode!.id!);
    }
    const validate = await validateDataCollection(ids);
    if (!validate) {
      message.warn('批量选择的数据集表结构必须相同，请重新选择');
      return false;
    }

    setStepCurrentSt(stepCurrentSt + 1);
    return true;
  };
  /**
   * 添加链路，步骤2:
   */
  const onStep2 = async () => {
    if (!sinkSrListSt || sinkSrListSt.length <= 0) {
      message.warn('目标端数据集必须选择，并且不能重复选择');
      return false;
    }

    // 1. 加载字段映射数据源
    const { resourceNode: sourceDc } = srcFirstSrSt!;
    const { resourceNode: sinkDc } = sinkFirstSrSt!;
    const dataSource = await generateConnectionColumnConf(sourceDc!.id!, sinkDc!.id!);

    // 2. 更新columnConf组件数据
    setConnClConfWhenAddSt({
      displayDataSource: dataSource,
      originalDataSource: dataSource,
      canEdit: sinkDc!.dataSystemType != DataSystemTypeConstant.KAFKA,
      canDelete: sinkDc!.dataSystemType == DataSystemTypeConstant.KAFKA,
      sinkDataSystemType: sinkDc!.dataSystemType,
      sinkDataCollectionId: sinkDc!.id,
      sourceDataCollectionId: sourceDc!.id,
    });

    setStepCurrentSt(stepCurrentSt + 1);
    return true;
  };

  let starrocks_support_table_model = new Set(['DUP_KEYS', 'PRIMARY_KEYS', 'UNIQUE_KEYS']);

  async function checkTableModelAndUk(
    sinkDataCollectionId: number,
    clConf: API.ConnectionColumnConf[],
    sinkDataSystemType: string,
  ) {
    let definition = (await getDataCollectionDefinition({
      id: sinkDataCollectionId,
    })) as API.DataCollectionDefinition;
    let { extendProperties } = definition;
    let checkResult = true;
    if (
      extendProperties &&
      extendProperties['TABLE_MODEL'] &&
      !starrocks_support_table_model.has(extendProperties['TABLE_MODEL'])
    ) {
      message.warn('Now starrocks only support primary/unique/duplicate sink tables.');
      checkResult = false;
    }

    if (
      !verifyUKWithShowMessage(
        clConf!,
        sinkDataSystemType,
        extendProperties && extendProperties['TABLE_MODEL'],
      )
    ) {
      checkResult = false;
    }
    return checkResult;
  }

  /**
   * 添加链路，步骤3:
   */
  const onStep3 = async () => {
    let clConf = connClConfWhenAddRef.current?.getRowsData?.();

    let sinkDataSystemType = sinkFirstSrSt?.resourceNode?.dataSystemType!;

    let sinkDataCollectionId = connClConfWhenAddSt?.sinkDataCollectionId;

    if (!(await checkTableModelAndUk(sinkDataCollectionId!, clConf!, sinkDataSystemType))) {
      return false;
    }

    // kafka 序列化方式控制显示
    if (sinkFirstSrSt?.resourceNode?.dataSystemType == DataSystemTypeConstant.KAFKA) {
      setKafkaDataFormatHiddenSt(false);
    } else {
      setKafkaDataFormatHiddenSt(true);
    }

    // 清空kafka序列化方式和项目负责人的上次缓存记录
    setKafkaDataFormatSt(undefined);
    setSrcForceSetPrjSt(undefined);
    setRefreshVSt(refreshVSt + 1);

    // 重新设置项目负责人下拉选项值,kafka为固定的下拉选项值，不需要依赖状态更新
    setSrcForceSetPrjListSt(srcFirstSrSt?.projectNoes!);
    setStepCurrentSt(stepCurrentSt + 1);
    return true;
  };

  const generateTags = (record: SearchRecord) => {
    const etags: JSX.Element[] = [];
    const tags = record.tags;
    if (tags) {
      let i = 1;
      for (let tag of tags) {
        etags.push(
          <Tag color="cyan" key={T_ROW_KEY_PREFIX + record.key + i}>
            {tag}
          </Tag>,
        );
        i++;
      }
    }
    return etags;
  };

  /**
   * 获取 TIDB 数据库实例
   * */
  const getTidbInstancId = async (sinkSr: SearchRecord) => {
    const { resourceNode } = sinkSr;
    if (resourceNode?.dataSystemType == DataSystemTypeConstant.TIDB) {
      const tidbClusterId: number = resourceNode!.parent!.parent!.id;
      const resourceQuery: API.DataSystemResourceQuery = {
        pageSize: 100,
        deleted: false,
        current: 1,
        resourceTypes: [DataSystemResourceTypeConstant.TIDB_SERVER],
        parentResourceId: tidbClusterId,
      };

      const pagedResource = await pagedQueryDataSystemResource(resourceQuery);
      const resources = pagedResource?.data;
      const instances = resources ? resources : [];

      // TODO 前期先固定选择第一个实例
      return instances.length >= 1 ? instances[0].id : undefined;
    }
    return undefined;
  };

  /**
   * 添加链路，表单提交:
   */
  const onStepSubmit = async () => {
    if (!srcForceSetPrjSt) {
      message.warn('请选择项目负责人');
      return false;
    }
    const { resourceNode: sinkRnode, projectNoes: sinkPnodes } = sinkFirstSrSt!;
    if (DataSystemTypeConstant.KAFKA === sinkRnode?.dataSystemType && !kafkaDataFormatSt) {
      message.warn('请选择 KAFKA 序列化方式');
      return false;
    }

    // 添加链路，多个source 与单个 sink 组合
    let srcForceSetProject = srcForceSetPrjSt;
    let sinkForceSetProject = sinkPnodes[0];
    let originClConf = connClConfWhenAddSt?.originalDataSource!;
    let currentClConf = connClConfWhenAddRef.current?.getRowsData?.()!;
    let specificConf: string | undefined;
    let sinkInstanceId = await getTidbInstancId(sinkFirstSrSt!);
    if (DataSystemTypeConstant.KAFKA == sinkRnode?.dataSystemType)
      specificConf = JSON.stringify({ dataFormatType: kafkaDataFormatSt });
    const connReqRecords: ConnectionReqRecord[] = [];
    let tempMapping = new Map<string, string>();

    for (let r of connReqRecordListSt) {
      tempMapping.set(uK(r.source!, r.sink!), '');
      connReqRecords.push(r);
    }

    let index = beginRk();
    for (let srcSr of sourceSrListSt) {
      // 重复链路过滤
      if (tempMapping.has(uK(srcSr!, sinkFirstSrSt!))) {
        message.info(
          '存在重复链路: ' + srcSr.resourceNode?.name + ' > ' + sinkFirstSrSt?.resourceNode?.name,
        );
        continue;
      }

      const reqRecord: ConnectionReqRecord = {
        key: index,
        source: srcSr!,
        sink: sinkFirstSrSt!,
        srcForceSetProject: srcForceSetProject,
        sinkForceSetProject: sinkForceSetProject,
        specificConf: specificConf,
        originClConf: originClConf,
        currentClConf: currentClConf,
        sinkInstanceId: sinkInstanceId,
      };
      connReqRecords.push(reqRecord);
      index++;
    }

    addConnDwOnClose();
    loadRecords(connReqRecords);

    return true;
  };

  const loadRecords = (records: ConnectionReqRecord[]) => {
    const sort = (records: ConnectionReqRecord[]) => {
      records.sort((n1, n2) => {
        return n1.key - n2.key;
      });
    };

    sort(records);
    setConnReqRecordListSt(records);
    setLoadVSt(loadVSt + 1);
    setScrollSt(records.length == 0 ? {} : S_SCROLL);
  };

  const columns: ProColumns<ConnectionReqRecord>[] = [
    {
      render: (_text, record, _, _action) => [
        <>
          <div>
            <Space key={SOURCE + B_ROW_KEY_PREFIX + record.key}>
              {/*<strong>源端:</strong>*/}
              <Breadcrumb routes={record.source.routes} />
              {generateTags(record.source)}
            </Space>
            <p></p>
            <Space key={SINK + B_ROW_KEY_PREFIX + record.key}>
              {/*<strong>目标:</strong>*/}
              <Breadcrumb routes={record.sink.routes} />
              {generateTags(record.sink)}
            </Space>
            <p></p>
            {/*<Space>
              <Button
                type="dashed"
                ghost
                onClick={() => {
                  delConnOnClick(record);
                }}
              >
                删除
              </Button>
              <Button
                type="dashed"
                ghost
                style={{ marginRight: 50 }}
                onClick={() => {
                  editConnOnClick(record);
                }}
              >
                修改
              </Button>
              <Tag color="cyan">{record.source.resourceNode?.resourceType}</Tag>
              <Tag color="cyan">{record.sink.resourceNode?.resourceType}</Tag>
              <Tag color="cyan">{record.srcForceSetProject.owner}</Tag>
            </Space>*/}

            {
              <Space>
                <a
                  onClick={() => {
                    delConnOnClick(record);
                  }}
                >
                  删除
                </a>
                <a
                  style={{ marginRight: 50 }}
                  onClick={() => {
                    editConnOnClick(record);
                  }}
                >
                  修改
                </a>
                <span style={{ marginRight: 5 }}>源负责人:</span>
                <span>{record.srcForceSetProject.owner}</span>
              </Space>
            }
          </div>
        </>,
      ],
    },
  ];
  return (
    <>
      <ConfigProvider renderEmpty={customizeRenderEmpty}>
        <ProTable
          showHeader={false}
          columns={columns}
          params={{
            loadV: loadVSt,
          }}
          request={async () => {
            return {
              success: true,
              data: connReqRecordListSt,
            };
          }}
          onLoad={(_records) => {}}
          // 根据屏幕动态计算高度，出现滚动条
          scroll={scrollSt}
          footer={() => (
            <Space>
              <Button
                type="dashed"
                ghost
                onClick={() => {
                  setStepCurrentSt(0);
                  setAddConnDwOpenSt(true);
                }}
              >
                添加链路
              </Button>
              <Button
                hidden={false}
                type="dashed"
                disabled={reqSubmitBtnSt}
                ghost
                onClick={() => {
                  connReqSubmitBtnOnClick();
                }}
              >
                提交申请
              </Button>
              <Button
                hidden={false}
                type="dashed"
                ghost
                onClick={() => {
                  manageLinkBtnOnClick();
                }}
              >
                管理链路
              </Button>
            </Space>
          )}
          loading={false}
          options={false}
          search={false}
          pagination={false}
          rowKey={(record) => String(record.key)}
          onRow={(_record) => {
            return {
              onClick: () => {},
            };
          }}
        />
      </ConfigProvider>
      <Drawer title="添加链路" width={'100%'} onClose={addConnDwOnClose} open={addConnDwOpenSt}>
        <StepsForm
          current={stepCurrentSt}
          stepsProps={{ direction: 'horizontal' }}
          containerStyle={{ width: '100%', height: '100%' }}
          onFinish={async () => {
            return onStepSubmit();
          }}
          submitter={{
            render: (props) => {
              if (props.step === 0) {
                return (
                  <Button key="next1" type="primary" onClick={() => props.onSubmit?.()}>
                    下一步 {'>'}
                  </Button>
                );
              }

              if (props.step === 1) {
                return [
                  <Button
                    key="back0"
                    onClick={() => {
                      props.onPre?.();
                      setStepCurrentSt(0);
                    }}
                  >
                    上一步
                  </Button>,
                  <Button type="primary" key="next2" onClick={() => props.onSubmit?.()}>
                    下一步 {'>'}
                  </Button>,
                ];
              }
              if (props.step === 2) {
                return [
                  <Button
                    key="back1"
                    onClick={() => {
                      props.onPre?.();
                      setStepCurrentSt(1);
                    }}
                  >
                    上一步
                  </Button>,
                  <Button type="primary" key="next3" onClick={() => props.onSubmit?.()}>
                    下一步 {'>'}
                  </Button>,
                ];
              }
              return [
                <Button
                  key="back2"
                  onClick={() => {
                    props.onPre?.();
                    setStepCurrentSt(2);
                  }}
                >
                  {'<'} 上一步
                </Button>,
                <Button type="primary" key="save" onClick={() => props.onSubmit?.()}>
                  保存 √
                </Button>,
              ];
            },
          }}
        >
          <StepsForm.StepForm
            title="选择源数据集"
            name="step1"
            onFinish={async () => {
              return onStep1();
            }}
          >
            <DcSearcher
              reqPageSize={20}
              searchScope={SearchScope.ALL}
              multipleChoice={true}
              validateProjectOwner={true}
              includedDataSystemTypes={[DataSystemTypeConstant.TIDB, DataSystemTypeConstant.MYSQL, DataSystemTypeConstant.ACDC_WIDE_TABLE]}
              rootResourceTypes={[
                DataSystemResourceTypeConstant.MYSQL_CLUSTER,
                DataSystemResourceTypeConstant.TIDB_CLUSTER,
                DataSystemResourceTypeConstant.ACDC_WIDE_TABLE,
              ]}
              onSelect={(records) => {
                setSourceSrListSt(records);
                if (records && records.length > 0) setSrcFirstSrSt(records[0]);
              }}
            />
          </StepsForm.StepForm>
          <StepsForm.StepForm
            title="选择目标端数据集"
            name="step2"
            onFinish={async () => {
              return onStep2();
            }}
          >
            <DcSearcher
              reqPageSize={20}
              searchScope={SearchScope.CURRENT_USER}
              multipleChoice={false}
              validateProjectOwner={false}
              includedDataSystemTypes={[
                DataSystemTypeConstant.TIDB,
                DataSystemTypeConstant.MYSQL,
                DataSystemTypeConstant.HIVE,
                DataSystemTypeConstant.KAFKA,
                DataSystemTypeConstant.ELASTICSEARCH,
                DataSystemTypeConstant.STARROCKS,
              ]}
              rootResourceTypes={[
                DataSystemResourceTypeConstant.MYSQL_CLUSTER,
                DataSystemResourceTypeConstant.TIDB_CLUSTER,
                DataSystemResourceTypeConstant.KAFKA_CLUSTER,
                DataSystemResourceTypeConstant.HIVE,
                DataSystemResourceTypeConstant.ELASTICSEARCH_CLUSTER,
                DataSystemResourceTypeConstant.STARROCKS_CLUSTER,
              ]}
              onSelect={(dcList) => {
                setSinkSrListSt(dcList);
                if (dcList && dcList.length > 0) setSinkFirstSrSt(dcList[0]);
              }}
            />
          </StepsForm.StepForm>
          <StepsForm.StepForm
            title="配置字段映射"
            name="step3"
            onFinish={async () => {
              return onStep3();
            }}
          >
            <ConnectionColumnConf
              columnConfProps={{ ...connClConfWhenAddSt }}
              editorFormRef={connClConfWhenAddRef}
            />
          </StepsForm.StepForm>
          <StepsForm.StepForm<string> title="数据链路配置" onFinish={async () => {}}>
            <Card hoverable bordered style={{ marginBottom: 100 }}>
              <div style={{ width: '100%', marginBottom: 30, marginTop: 30 }}>
                <span style={{ width: '20%', marginRight: 10 }}>项目负责人:</span>
                <Select
                  /*TODO 临时解决更新了下拉内容，上一次选中的内容不能取消的bug*/
                  key={'s1_' + refreshVSt}
                  showSearch
                  showArrow
                  placeholder="请选择项目负责人"
                  style={{ width: '46%' }}
                  optionFilterProp="children"
                  onSelect={(item) => {
                    let pNode: ProjectNode;
                    for (let p of srcForceSetPrjListSt) {
                      if (p.id === item) {
                        pNode = p;
                        break;
                      }
                    }
                    setSrcForceSetPrjSt(pNode!);
                  }}
                  virtual={true}
                >
                  {srcForceSetPrjListSt.map((item) => (
                    <Option value={item.id} key={item.id}>
                      <strong>{item.owner}</strong>&nbsp;&nbsp;&nbsp;&nbsp;<span>{item.name}</span>
                    </Option>
                  ))}
                </Select>
              </div>

              <div
                style={{ width: '100%', marginBottom: 30, marginTop: 30 }}
                hidden={kafkaDataFormatHiddenSt}
              >
                <span style={{ width: '20%', marginRight: 10 }}>序列化方式:</span>
                <Select
                  key={'s1_' + refreshVSt}
                  showSearch
                  showArrow
                  placeholder="请选择 KAFKA 序列化方式"
                  style={{ width: '46%' }}
                  optionFilterProp="children"
                  onSelect={(item) => setKafkaDataFormatSt(item)}
                  virtual={true}
                >
                  {['CDC_V1', 'JSON', 'SCHEMA_LESS_JSON'].map((item) => (
                    <Option value={item} key={item}>
                      {item}
                    </Option>
                  ))}
                </Select>
              </div>
            </Card>
          </StepsForm.StepForm>
        </StepsForm>
      </Drawer>
      <DrawerForm<{}>
        title="字段编辑"
        visible={editConnDwOpenSt}
        width={'100%'}
        drawerProps={{
          forceRender: false,
          destroyOnClose: true,
          onClose: () => {
            editConnDwOnClose();
          },
        }}
        onFinish={async () => {
          return editConnDwOnSubmit(editConnReqRecordSt);
        }}
        onInit={() => {}}
      >
        <ConnectionColumnConf
          columnConfProps={{
            ...connClConfWhenEditSt,
          }}
          editorFormRef={connClConfWhenEditRef}
        />
      </DrawerForm>

      <ModalForm
        title="申请理由"
        visible={reqReasonDwOpenSt}
        modalProps={{
          destroyOnClose: true,
          onCancel: () => {
            reqReasonDwOnCancel();
          },
        }}
        onFinish={reqReasonDwOnSubmit}
        onInit={() => {}}
      >
        <ProForm<{
          description?: string;
        }>
          formRef={reqReasonDwRef}
          formKey="reqReasonDwForm"
          autoFocusFirstInput
          submitter={{
            // 配置按钮文本
            searchConfig: {
              resetText: '重置',
              submitText: '提交',
            },
            // 配置按钮的属性
            resetButtonProps: {
              style: {
                // 隐藏重置按钮
                display: 'none',
              },
            },
            // 隐藏提交按钮
            submitButtonProps: {},
            // 完全自定义整个区域
            render: (_props, _doms) => {
              return [];
            },
          }}
        >
          <ProFormTextArea
            name="description"
            label="请输入申请理由"
            placeholder="请输入申请理由"
            rules={[{ required: true, max: 200, message: '请填写申请理由,200字符以内' }]}
          />
        </ProForm>
      </ModalForm>
    </>
  );
};

export default ConnctoinRequisition;
