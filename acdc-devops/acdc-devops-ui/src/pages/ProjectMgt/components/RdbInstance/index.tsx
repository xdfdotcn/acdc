import {
  pagedQueryDataSystemResource,
  updateChildDataSystemResources} from '@/services/a-cdc/api';
import {EditOutlined} from '@ant-design/icons';
import ProForm from '@ant-design/pro-form';
import {EditableFormInstance, EditableProTable, ProColumns} from '@ant-design/pro-table';
import {message, Modal} from 'antd';
import {useRef, useState} from 'react';
import {useModel} from 'umi';
import {DataSystemResourceTypeConstant} from "@/services/a-cdc/constant/DataSystemResourceTypeConstant";
import {DataSystemTypeConstant} from "@/services/a-cdc/constant/DataSystemTypeConstant";

const {confirm} = Modal;

const RdbInstance: React.FC = () => {

  // 数据流model,接受来自任何页面的传参
  const {rdbInstanceModel} = useModel('RdbInstanceModel')

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

  /**初始化 rcordId*/
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
    const tableRdbInstances = editorFormRef.current?.getFieldValue('table') as API.RdbInstance[];

    // 校验: 必须录入实例
    if (!tableRdbInstances || tableRdbInstances.length <= 0) {
      message.warn("请添加实例")
      return
    }

    // 校验:录入实例重复
    let rdbInstanceMap: Map<string, string> = new Map([])
    let masterInstanceCount = 0
    let dataSourceInstanceCount = 0
    for (let i = 0; i < tableRdbInstances.length; i++) {
      let item = tableRdbInstances[i]
      let itemHost = item.host;
      let itemPort = item.port;
      let uniqueKey: string = itemHost + ":" + itemPort

      if (rdbInstanceMap.get(uniqueKey)) {
        message.warn("重复录入, " + uniqueKey)
        return
      }

      if (rdbInstanceModel.dataSystemType == DataSystemTypeConstant.MYSQL) {
        if ('MASTER' == item.roleType) {
          masterInstanceCount++
        }

        if ('DATA_SOURCE' == item.roleType) {
          dataSourceInstanceCount++
        }
      }

      rdbInstanceMap.set(uniqueKey, uniqueKey)
    }

    // mysql 校验: 主库只能录入一个
    if (rdbInstanceModel.dataSystemType == DataSystemTypeConstant.MYSQL) {
      if (masterInstanceCount > 1) {
        message.warn("只能录入一个主库")
        return
      }

      if (dataSourceInstanceCount > 1) {
        message.warn("只能录入一个CDC数据源")
      }
    }

    confirm({
      title: '确定提交吗',
      icon: <EditOutlined/>,
      content: '修改实例信息',
      async onOk() {
        let body: API.DataSystemResource[] = [];
        tableRdbInstances.forEach((val, idx, arr) => {
            let host = val.host
            let port = val.port;
            let newHost = host?.replace(/(^\n)|(\t)|(\s)/g, "");

            // configurations
            const usernameConfiguration: API.DataSystemResourceConfiguration = {
              name: "host",
              value: newHost
            }
            const passwordConfiguration: API.DataSystemResourceConfiguration = {
              name: "port",
              value: port
            }
            const dataSystemResourceConfigurations = {
              "host": usernameConfiguration,
              "port": passwordConfiguration
            };

            const dataSystemResource: API.DataSystemResource = {
              name: newHost + ":" + port,
              dataSystemType: rdbInstanceModel.dataSystemType,
              parentResourceId: rdbInstanceModel.resourceId,
              dataSystemResourceConfigurations: dataSystemResourceConfigurations,
            }

            if (rdbInstanceModel.dataSystemType == DataSystemTypeConstant.MYSQL) {
              dataSystemResource.resourceType = DataSystemResourceTypeConstant.MYSQL_INSTANCE
              const roleTypeConfiguration: API.DataSystemResourceConfiguration = {
                name: "role_type",
                value: val.roleType
              }
              dataSystemResourceConfigurations.role_type = roleTypeConfiguration
            } else if (rdbInstanceModel.dataSystemType == DataSystemTypeConstant.TIDB) {
              dataSystemResource.resourceType = DataSystemResourceTypeConstant.TIDB_SERVER
            }

            body.push(dataSystemResource)
          }
        )
        updateChildDataSystemResources(rdbInstanceModel.resourceId, body)
        message.info("修改成功")
      },
      onCancel() {
      },
    });
  }

  // 编辑表格列声明
  const columns: ProColumns<API.RdbInstance>[] = [
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
      title: '实例端口',
      dataIndex: 'port',
      valueType: 'digit',
      formItemProps: () => {
        return {
          rules: [{required: true, message: '此项为必填项'}],
        };
      },
      width: '30%',
    },

    {
      title: '角色',
      dataIndex: 'roleType',
      formItemProps: () => {
        return {
          rules: [{required: true, message: '此项为必填项'}],
        };
      },
      editable: rdbInstanceModel.dataSystemType == DataSystemTypeConstant.MYSQL,
      valueType: 'select',
      valueEnum: {
        MASTER: {text: '主库', status: 'Error'},
        SLAVE: {
          text: '从库',
          status: 'Default',
        },
        DATA_SOURCE: {
          text: 'CDC数据源',
          status: 'Success',
        },
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
              content: '删除用户',
              async onOk() {
                const currentTableItems = editorFormRef.current?.getFieldValue('table') as API.RdbInstance[];
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
          table: API.RdbInstance[]
        }>
          onFinish={() => {
            submitForm()
          }}
          validateTrigger="onBlur"
        >
          <EditableProTable<API.RdbInstance, API.DataSystemResourceQuery>
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
              parentResourceId: rdbInstanceModel.resourceId,
              resourceTypes: [DataSystemResourceTypeConstant.MYSQL_INSTANCE, DataSystemResourceTypeConstant.TIDB_SERVER],
              pageSize: 100
            }}
            request={pagedQueryDataSystemResource}
            postData={(data: API.DataSystemResource[]) => {
              let rdbInstances: API.RdbInstance[] = [];
              data.forEach((value, number, data) => {
                let rdbInstance: API.RdbInstance = {
                  id: value.id,
                  host: value.dataSystemResourceConfigurations.host.value,
                  port: value.dataSystemResourceConfigurations.port.value
                }
                if (value.dataSystemResourceConfigurations.role_type) {
                  rdbInstance.roleType = value.dataSystemResourceConfigurations.role_type.value
                }
                rdbInstances.push(rdbInstance)
              })
              return rdbInstances
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

export default RdbInstance;
