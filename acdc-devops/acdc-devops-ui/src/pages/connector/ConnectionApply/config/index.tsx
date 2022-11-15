import React, { useEffect, useState } from 'react';
import { Form, message, Select, Modal, Table } from 'antd';
import { reject } from 'lodash';
import { history, useModel } from 'umi';
import type {ProColumns} from '@ant-design/pro-table';
import {fetchFieldMapping, fetchDataSetFields} from '@/services/a-cdc/api';
import Step5 from '@/pages/connector/ConnectionApply/create/step5';
import { handleFieldMappingItem, verifyPrimaryKey } from '@/services/a-cdc/connector/field-mapping';
import {EditableProTable} from '@ant-design/pro-table';
import styles from './index.less';
import { DatabaseFilled } from '@ant-design/icons';

const { Option } = Select;

interface FormProps {
  selectItem: any;
  modalVisible: boolean;
  onCancel: () => void;
  onSubmit: (data: any) => void;

}

const UN_REUSABLE_META_FIELDS_MAP = new Map([
	['__logical_del\tstring', ''],
]);
const HIVE_REUSABLE_META_FIELDS_MAP = new Map([
	['', ''],
	['__op\tstring', ''],
	['__kafka_record_offset\tstring', ''],
	['__datetime\tstring', ''],
]);
const RDB_REUSABLE_META_FIELDS_MAP = new Map([
	['', ''],
	['__datetime\tstring', ''],
]);
const ALL_META_FIELDS_MAP = new Map([
	['', ''],
	['__op\tstring', ''],
	['__kafka_record_offset\tstring', ''],
	['__logical_del\tstring', ''],
	['__datetime\tstring', ''],
]);



const ConfigForm: React.FC<FormProps>= (props)=>{
  const { selectItem, modalVisible, onCancel, onSubmit } = props;
  const { fieldMappings } = selectItem;
  const { fieldMappingModel, setFieldMappingModel } = useModel('FieldMappingModel');

	// 可编辑表格
	const [editableKeys, setEditableRowKeys] = useState<React.Key[]>(() => []);
	// 下拉菜单
	const [fieldMenu, setFieldMenu] = useState<string[]>([]);
	//列表数据
	const  [dataSource, setDataSource] = useState<[]>(() => dataSource);
  const [form] = Form.useForm();
	// 页面初始化之后保存的所有Item 数据
  useEffect(()=>{
		if(modalVisible){
			setDataSource(fieldMappings)
		}
	},[modalVisible])
  // 可编辑表格列声明
	const fieldMappingColumns: ProColumns<API.FieldMappingListItem>[] = [
		{
			title: '目标字段',
			dataIndex: 'sinkFieldFormat',
			valueType: "text",
			width: "35%",
			editable: false,
		},
		{
			title: '源字段',
			dataIndex: 'sourceFieldFormat',
			valueType: "select",
			hideInTable: selectItem.sinkDataSystemType == 'KAFKA',
			width: "35%",
			renderFormItem: (record, {defaultRender, ...rest}, form) => {
				return (
					<Select
						showSearch
						placeholder="请选择"
						style={{minWidth: 200}}
						optionFilterProp="children"
						onSelect={(item) => {}}
						virtual={false}
					>
						{/** 这个里面需要做过滤*/}
						{
							fieldMenu.map((item) =>
								<Option value={item} key={item}> {item} </Option>)
						}
					</Select>
				)
			}
		},
		{
			title: '过滤条件',
			dataIndex: 'filterOperator',
			valueType: 'select',
			width: "10%",
			valueEnum: {
				'<': {text: '<', value: '<'},
				'<=': {text: '<=', value: '<='},
				'==': {text: '==', value: '=='},
				'!=': {text: '!=', value: '!='},
				'>=': {text: '>=', value: '>='},
				'>': {text: '>', value: '>'},
			},
		},
		{
			title: '过滤值',
			width: "10%",
			dataIndex: 'filterValue',
		},
		{
			width: "10%",
			valueType: 'option',
			render: (text, record, _, action) => [
				<a
					key="editable"
					onClick={() => {
						if (record.id) {
							action?.startEditable?.(record.id);
						}
					}}
				>
					编辑
				</a>

			],
		},
	];
	const appendMetaFieldToMenu = (
		newMenu: string[],
		unReusableMetaFieldMap: Map<string, string>
	) => {
		let sinkDataSystemType = fieldMappingModel.sinkDataSystemType;
		if (!sinkDataSystemType) {
			return
		}

		if (sinkDataSystemType == 'HIVE') {
			HIVE_REUSABLE_META_FIELDS_MAP.forEach((val, key, map) => {
				newMenu.push(key)
			})
		} else {// RDB
			RDB_REUSABLE_META_FIELDS_MAP.forEach((val, key, map) => {
				newMenu.push(key)
			})

			unReusableMetaFieldMap.forEach((val, key, map) => {
				newMenu.push(key)
			})
		}
	}
	/**
		查找源数据集列表
	*/
	const fetchFieldMenu = async (editableKeys, editableRows) => {
		// 源表id
		const key = editableKeys[0];
		const srcDataSetId = selectItem.srcDataSetId;

		// 加载下拉菜单数据
		const fieldList = await fetchDataSetFields({id: srcDataSetId});
		const sourceFieldMap: Map<string, string> = new Map([]);
		selectItem.fieldMappings.forEach((val, index, arr) => {
			if (val && val.sourceFieldFormat) {
				sourceFieldMap.set(val.sourceFieldFormat, val.sourceFieldFormat);
			}
		})

		let menu: string[] | undefined = fieldList?.filter((val, index, arr) => {
			return !sourceFieldMap.get(val)
		})

		menu = !menu ? [] : menu;

		const newMenu: string[] = []

		appendMetaFieldToMenu(newMenu,UN_REUSABLE_META_FIELDS_MAP)

		// 不可重复使用的元数据填充
		menu.forEach((val, idx, arr) => {
			newMenu.push(val)
		})

		setFieldMenu(newMenu);
	}


	const editDataSource = (
		curItem: API.FieldMappingListItem,
		oldItem: API.FieldMappingListItem) => {

		if (dataSource && dataSource.length > 0) {
			const copy = dataSource;
			// 找到对应数据并且进行修改,然后重新放到数组中
			copy.forEach((record, index, arr) => {
				if (curItem.id == record.id) {
					arr[index] = curItem;
				}
			})
			setDataSource(copy);
		}

		// 本次操作与上一次选择的字段字段相同
		if (curItem.sourceFieldFormat == oldItem.sourceFieldFormat) {
			return;
		}
		// 当前菜单数据转换成map
		let menu = !fieldMenu ? [] : fieldMenu;
		let menuMap: Map<string, string> = new Map([]);
		menu.forEach((val, idx, arr) => {
			menuMap.set(val, val)
		})

		// 删除当前选中字段
		if (curItem && curItem.sourceFieldFormat) {
			menuMap.delete(curItem.sourceFieldFormat)
		}

		// 还原上一次的选中字段
		if (oldItem && oldItem.sourceFieldFormat) {
			menuMap.set(oldItem.sourceFieldFormat, oldItem.sourceFieldFormat)
		}

		// 获取元数据字段和普通字段
		let unReusableMetafields: Map<string, string>=new Map([])
		let normalFields: string[] = []

		for (let entity of menuMap.entries()) {
			if (UN_REUSABLE_META_FIELDS_MAP.has(entity[0])) {
				unReusableMetafields.set(entity[0], entity[0])
			} else if (!ALL_META_FIELDS_MAP.has(entity[0])) {
				normalFields.push(entity[0])
			} else {
				// no nothing
			}
		}

		// 新 menu
		let newMenu: string[] = []

		// 元数据字段填充
		appendMetaFieldToMenu(newMenu, unReusableMetafields)

		// 普通数据填充
		normalFields.forEach((val, idx, arr) => {
			newMenu.push(val)
		})

		// 更新state
		setFieldMenu(newMenu)

	}
  const onConfirm = () => {
    const fieldMappings: API.FieldMappingListItem[] = fieldMappingModel!.fieldMappings!;
    // 主键字段校验
    if (!verifyPrimaryKey(fieldMappings, selectItem.sinkDataSystemType!)) {
      if (selectItem.sinkDataSystemType == 'HIVE'
        || selectItem.sinkDataSystemType == 'KAFKA'
      ) {
        message.warn("源表不存在主键字段")
      }

      if (selectItem.sinkDataSystemType == 'MYSQL'
        || selectItem.sinkDataSystemType == 'TIDB'
      ) {
        message.warn("源表主键与目标表主键类型不一致")
      }
      // return onSubmit(true);
    }
		const copy = selectItem;
		copy.fieldMappings = dataSource;
    return onSubmit(copy);
  }
  return (
    <Modal
      destroyOnClose
      title="配置"
      visible={modalVisible}
      okText="提交"
      cancelText="取消"
      onCancel={() => onCancel()}
      onOk={onConfirm}
      width="80%"
    //   footer={null}
    >
      {/* <Step5/> */}
      <EditableProTable<API.FieldMappingListItem, API.FieldMappingQuery>
        rowKey="id"
				controlled={true}
        maxLength={5}
        // 关闭默认的新建按钮
        recordCreatorProps={false}
        columns={fieldMappingColumns}
        value={dataSource}
        rowClassName={(record) => {
          if (selectItem.sinkDataSystemType == 'KAFKA') {
            return
          }
          return record.sourceFieldFormat === '' ? styles['split-row-select-active'] : '';
        }}
				editable={{
					form,
					editableKeys,
					onSave: async (index, cur, old) => {
						if (cur && cur.filterOperator) {
							const conditionValue = cur?.filterValue
							const isMetaField = cur.sourceFieldFormat!.startsWith('__');
							// 暂时先做非空处理,后续根据情况修改正则
							const len = conditionValue?.replace(/(^\s*)|(\s*$)/g, "").length;
							const isNullExpression = (!len || len <= 0)
							const isNoSrcField = (!cur.sourceFieldFormat)

							if (isMetaField || isNullExpression || isNoSrcField) {
								cur.filterValue = ''
								cur.filterOperator = ''
							}

							// message
							if (isMetaField) {
								message.warn('"ACDC 字段",不能配置列过滤条件')
							} else if (isNoSrcField) {
								message.warn('配置列过滤条件,源字段不能为空')

							} else if (isNullExpression) {
								message.warn('配置列过滤条件,过滤值不能为空')
							} else {
								// pass
							}
						}
						// 判断如果输入了条件,但是没有输入值则报错
						await editDataSource(cur, old);
					},
					onValuesChange: (record, recordList) => {
            setDataSource(recordList);
          },
					onChange: (editableKeys: Key[], editableRows: T[]) => { fetchFieldMenu(editableKeys, editableRows); return setEditableRowKeys(editableKeys)},
					actionRender: (row, config, dom) => [dom.save, dom.cancel],
				}}
      />
			{/* <Table
			dataSource={fieldMappings} columns={fieldMappingColumns}
			/> */}
    </Modal>
  )
}

export default ConfigForm;
