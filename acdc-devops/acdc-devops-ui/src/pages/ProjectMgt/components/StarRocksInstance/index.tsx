import {pagedQueryDataSystemResource, updateChildDataSystemResources} from '@/services/a-cdc/api';
import {EditOutlined} from '@ant-design/icons';
import ProForm from '@ant-design/pro-form';
import {EditableFormInstance, EditableProTable, ProColumns} from '@ant-design/pro-table';
import {message, Modal} from 'antd';
import {useRef, useState} from 'react';
import {useModel} from 'umi';
import {DataSystemResourceTypeConstant} from "@/services/a-cdc/constant/DataSystemResourceTypeConstant";

const {confirm} = Modal;

const StarRocksInstance: React.FC = () => {

  // 数据流model,接受来自任何页面的传参
  const {starRocksInstanceModel} = useModel('StarRocksInstanceModel')

  // 编辑表格启动编辑模式,固定的state触发
  const [editableKeys, setEditableRowKeys] = useState<React.Key[]>(() => []);

  /**
   1. 编辑表格,增加record记录需要使用唯一key,每次都需要累加,保证不重复
   2. 初始化值为表格中所有记录中record id 最大的值
   3. 每次增加一条记录都需要累加此值
   4. 每次添加record记录的时候默认返回最新的值,保证动态添加一定会有不重复的唯一
   key
   */
  const [recordId, setRecordId] = useState<number>(-99)

  // 使用form ref 获取表格中的数据,这样使用起来更加方便,不用额外使用state
  const editorFormRef = useRef<EditableFormInstance<API.DataSystemResource>>();

  /**初始化 recordId*/
  const initRecordId = async (items: API.DataSystemResource[]) => {
    let maxId = 0
    for (let i = 0; i < items.length; i++) {
      if (maxId < items[i].id!) {
        maxId = items[i].id!
      }
    }
    setRecordId(maxId + 1)
  }

  /**提交表单*/
  const submitForm = async () => {
    const tableStarRocksInstances = editorFormRef.current?.getFieldValue('table') as API.StarRocksInstance[];

    // 校验: 必须录入实例
    if (!tableStarRocksInstances || tableStarRocksInstances.length <= 0) {
      message.warn("请添加实例")
      return
    }

    // 校验:录入实例重复
    let starRocksInstanceMap: Map<string, string> = new Map([])
    for (let i = 0; i < tableStarRocksInstances.length; i++) {
      let item = tableStarRocksInstances[i]
      let itemHost = item.host;
      let itemJdbcPort = item.jdbcPort;
      let uniqueKey: string = itemHost + ":" + itemJdbcPort

      if (starRocksInstanceMap.get(uniqueKey)) {
        message.warn("重复录入, " + uniqueKey)
        return
      }

      starRocksInstanceMap.set(uniqueKey, uniqueKey)
    }

    confirm({
      title: '确定提交吗',
      icon: <EditOutlined/>,
      content: '修改实例信息',
      async onOk() {
        let body: API.DataSystemResource[] = [];
        tableStarRocksInstances.forEach((val, idx, arr) => {
            let host = val.host
            let jdbcPort = val.jdbcPort;
            let httpPort = val.httpPort;
            let type = val.type;
            let newHost = host?.replace(/(^\n)|(\t)|(\s)/g, "");

            // configurations
            const hostConfiguration: API.DataSystemResourceConfiguration = {
              name: "host",
              value: newHost
            }
            const jdbcPortConfiguration: API.DataSystemResourceConfiguration = {
              name: "jdbcPort",
              value: jdbcPort
            }
          const httpPortConfiguration: API.DataSystemResourceConfiguration = {
            name: "httpPort",
            value: httpPort
          }
          const typeConfiguration: API.DataSystemResourceConfiguration = {
            name: "type",
            value: type
          }
            const dataSystemResourceConfigurations = {
              "host": hostConfiguration,
              "jdbcPort": jdbcPortConfiguration,
              "httpPort": httpPortConfiguration,
              "type": typeConfiguration
            };

            const parentResource: API.DataSystemResource = {
              id: starRocksInstanceModel.resourceId
            }

            const dataSystemResource: API.DataSystemResource = {
              name: newHost + ":" + jdbcPort,
              dataSystemType: starRocksInstanceModel.dataSystemType,
              parentResource: parentResource,
              dataSystemResourceConfigurations: dataSystemResourceConfigurations,
            }

          dataSystemResource.resourceType = DataSystemResourceTypeConstant.STARROCKS_FRONTEND
            body.push(dataSystemResource)
          }
        )
        updateChildDataSystemResources(starRocksInstanceModel.resourceId, body)
        message.info("修改成功")
      },
      onCancel() {
      },
    });
  }

  // 编辑表格列声明
  const columns: ProColumns<API.StarRocksInstance>[] = [
    {
      title: '实例地址',
      dataIndex: 'host',
      formItemProps: () => {
        return {
          rules: [{required: true, message: '此项为必填项'}],
        };
      },
      width: '30%',
    },
    {
      title: 'http端口',
      dataIndex: 'httpPort',
      valueType: 'digit',
      formItemProps: () => {
        return {
          rules: [{required: true, message: '此项为必填项'}],
        };
      },
      width: '20%',
    },
    {
      title: 'jdbc端口',
      dataIndex: 'jdbcPort',
      valueType: 'digit',
      formItemProps: () => {
        return {
          rules: [{required: true, message: '此项为必填项'}],
        };
      },
      width: '20%',
    },
    {
      title: '类型',
      dataIndex: 'type',
      formItemProps: () => {
        return {
          rules: [{required: true, message: '此项为必填项'}],
        };
      },
      valueType: 'select',
      valueEnum: {
        FE: {text: 'FE', status: 'Default'},
        BE: {text: 'BE', status: 'Default'},
      },
      width: '30%',
    },

    {
      title: '操作',
      valueType: 'option',
      width: 200,
      render: (text, record, _, action) => [
        <a
          key="editable"
          onClick={() => {
            action?.startEditable?.(record.id);
          }}
        >
          编辑
        </a>,
        <a
          key="delete"
          onClick={() => {
            confirm({
              title: '确定删除吗',
              icon: <EditOutlined/>,
              content: '删除已维护实例',
              async onOk() {
                const currentTableItems = editorFormRef.current?.getFieldValue('table') as API.StarRocksInstance[];
                let newTableItems = currentTableItems.filter((item) => item.id !== record?.id)
                editorFormRef.current?.setFieldsValue({
                  table: newTableItems,
                });
                message.success('操作成功');
              },
              onCancel() {
              },
            });
          }}
        >
          删除
        </a>,
      ],
    },
  ];

  return (
    <div>
      <>
        <ProForm<{
          table: API.StarRocksInstance[]
        }>
          onFinish={() => {
            submitForm()
          }}
          validateTrigger="onBlur"
        >
          <EditableProTable<API.StarRocksInstance, API.DataSystemResourceQuery>
            rowKey="id"
            scroll={{
              x: 960,
            }}
            editableFormRef={editorFormRef}
            maxLength={50}
            name="table"
            recordCreatorProps={
              {
                position: 'top',
                record: (__, records) => {
                  return {id: recordId}
                },
              }
            }
            columns={columns}
            params={{
              parentResourceId: starRocksInstanceModel.resourceId,
              resourceTypes: [DataSystemResourceTypeConstant.STARROCKS_FRONTEND],
              pageSize: 100
            }}
            request={pagedQueryDataSystemResource}
            postData={(data: API.DataSystemResource[]) => {
              let starRocksInstances: API.StarRocksInstance[] = [];
              data.forEach((value, number, data) => {
                let starRocksInstance: API.StarRocksInstance = {
                  id: value.id,
                  host: value.dataSystemResourceConfigurations?.host.value,
                  jdbcPort: value.dataSystemResourceConfigurations?.jdbcPort.value,
                  httpPort: value.dataSystemResourceConfigurations?.httpPort.value,
                  type: value.dataSystemResourceConfigurations?.type.value
                }
                starRocksInstances.push(starRocksInstance)
              })
              return starRocksInstances
            }}
            onLoad={(items) => {
              initRecordId(items)
            }}
            editable={{
              type: 'single',
              editableKeys,
              onChange: setEditableRowKeys,
              actionRender: (row, config, defaultDom) => {
                return [
                  defaultDom.save,
                  defaultDom.delete || defaultDom.cancel,
                ];
              },

              onSave: async (index, cur, old) => {
                // 更新最大的recordId,防止id key 重复
                setRecordId(recordId + 1)
              },
            }}
          />
        </ProForm>
      </>
    </div>
  )
};

export default StarRocksInstance;
