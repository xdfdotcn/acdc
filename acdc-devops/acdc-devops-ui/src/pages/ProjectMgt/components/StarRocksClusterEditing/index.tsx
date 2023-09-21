import React, {useEffect, useRef} from 'react';
import ProForm, {DrawerForm, ProFormInstance, ProFormText} from '@ant-design/pro-form';
import {useModel} from 'umi';
import {EditOutlined} from '@ant-design/icons';
import {message, Modal} from 'antd';
import {createDataSystemResource, getDataSystemResource, updateDataSystemResource} from '@/services/a-cdc/api';
import {ActionType} from "@ant-design/pro-table";

const {confirm} = Modal;

const StarRocksClusterEditing: React.FC<{tableRef: ActionType}> = ({tableRef}) => {
  // 数据流
  const {starRocksClusterEditingModel, setStarRocksClusterEditingModel} = useModel('StarRocksClusterEditingModel')
  // form ref
  const formRef = useRef<ProFormInstance<{
    name: string;
    description?: string;
    username?: string;
    password?: string;
  }>>();

//	发生在首次挂载(mount), 更新state不会再次触发
  useEffect(() => {
  }, [starRocksClusterEditingModel.resourceId]);

  const initFormValue = async () => {
    if (starRocksClusterEditingModel.from == 'create') {
      return;
    }
    // 表单数据
    let starRocks: API.DataSystemResource = await getDataSystemResource({id: starRocksClusterEditingModel.resourceId!})
    formRef?.current?.setFieldsValue({
      name: starRocks!.name,
      description: starRocks!.description,
      username: starRocks!.dataSystemResourceConfigurations?.username.value
    });
  }

  const submitForm = async () => {
    await formRef.current?.validateFields();
    confirm({
      title: '确定提交吗?',
      icon: <EditOutlined/>,
      content: '增加集群',
      async onOk() {
        const formObj = formRef.current?.getFieldsFormatValue?.()

        // build data system resource
        const project: API.Project = {
          id: starRocksClusterEditingModel.projectId
        }

        // configurations
        const usernameConfiguration: API.DataSystemResourceConfiguration = {
          name: "username",
          value: formObj!.username
        }
        const passwordConfiguration: API.DataSystemResourceConfiguration = {
          name: "password",
          value: formObj!.password
        }
        const dataSystemResourceConfigurations = {
          "username": usernameConfiguration,
          "password": passwordConfiguration
        };

        const dataSystemResource: API.DataSystemResource = {
          name: formObj!.name,
          dataSystemType: "STARROCKS",
          description: formObj!.description,
          projects: [project],
          dataSystemResourceConfigurations: dataSystemResourceConfigurations,
          resourceType: "STARROCKS_CLUSTER"
        }

        if ('create' == starRocksClusterEditingModel.from) {
          await createDataSystemResource(dataSystemResource)
          await tableRef.reload(false);
          message.info("添加成功")
        } else {
          dataSystemResource.id = starRocksClusterEditingModel.resourceId
          await updateDataSystemResource(dataSystemResource)
          await tableRef.reload(false);
          message.info("修改成功")
        }

        setStarRocksClusterEditingModel({
          ...starRocksClusterEditingModel,
          showDrawer: false
        })
      },
      onCancel() {
      },
    });
  }

  return (
    <div>
      <DrawerForm<{}>
        title="集群信息"
        visible={starRocksClusterEditingModel.showDrawer}
        width={"35%"}
        style={{position: 'absolute'}}
        drawerProps={{
          forceRender: false,
          destroyOnClose: true,
          mask: true,
          autoFocus: true,
          placement: 'right',
          onClose: () => {
            setStarRocksClusterEditingModel({
              ...starRocksClusterEditingModel,
              showDrawer: false
            })
          }
        }}
        onFinish={submitForm}
        onInit={() => {
          initFormValue()
        }}
      >

        <ProForm<{}>
          formRef={formRef}
          formKey="projectCreationForm"
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
            render: (props, doms) => {
              return [];
            },
          }}
        >

          <ProFormText
            width="md"
            name="name"
            label="集群名称"
            placeholder="请输入集群名称"
            rules={[{required: true, message: '集群名称必填'}]}
          />

          <ProFormText
            width="md"
            name="username"
            label="用户名"
            placeholder="请输入用户名"
            rules={[{required: true, message: '用户名必填'}]}
          />

          <ProFormText
            width="md"
            name="password"
            label="密码"
            placeholder="请输入密码"
            rules={[{required: true, message: '用户名必填'}]}
          />

          <ProFormText
            width="md"
            name="description"
            label="集群描述"
            placeholder="请输入集群描述信息"
            rules={[{required: true, message: '集群描述信息必填'}]}
          />
        </ProForm>
      </DrawerForm>
    </div>
  )
};

export default StarRocksClusterEditing;
