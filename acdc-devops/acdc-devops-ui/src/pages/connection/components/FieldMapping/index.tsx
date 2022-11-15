import React, {useState, useEffect, useRef} from 'react';
import type {ProColumns} from '@ant-design/pro-table';
import {EditableProTable} from '@ant-design/pro-table';
import {Form, message, Select} from 'antd';
import {fetchDataSetFields, fetchConnectionFieldMapping} from '@/services/a-cdc/api';
import styles from './split.less';
import {useModel} from 'umi';
const { Option } = Select;
const ConnectionFieldMappingList: React.FC = (props) => {
	// meta 字段

	const META_FIELDS_VALUE_MAP = new Map([
		['-', '__none\tstring'],
		['__op\tstring', '__op\tstring'],
		['__kafka_record_offset\tstring', '__kafka_record_offset\tstring'],
		['__logical_del\tstring', '__logical_del\tstring'],
		['__datetime\tstring', '__datetime\tstring'],
	]);

	const ALL_META_FIELDS_MAP = new Map([
		['', ''],
		['__op\tstring', ''],
		['__kafka_record_offset\tstring', ''],
		['__logical_del\tstring', ''],
		['__datetime\tstring', ''],
	]);

	const RDB_REUSABLE_META_FIELDS_MAP = new Map([
		['', ''],
		['__datetime\tstring', ''],
	]);

	const HIVE_REUSABLE_META_FIELDS_MAP = new Map([
		['', ''],
		['__op\tstring', ''],
		['__kafka_record_offset\tstring', ''],
		['__datetime\tstring', ''],
	]);


	const UN_REUSABLE_META_FIELDS_MAP = new Map([
		['__logical_del\tstring', ''],
	]);

	const {connectionFieldMappingModel, setConnectionFieldMappings} = useModel('ConnectionFieldMappingModel')

	const showHandleColumn: boolean = connectionFieldMappingModel.from == 'detail'

	// 可编辑表格
	const [editableKeys, setEditableRowKeys] = useState<React.Key[]>(() => []);
	const [form] = Form.useForm();

	// 页面初始化之后保存的所有Item 数据
	const [loadedTableItems] = useState<API.FieldMappingListItem[]>([]);

	// 下拉菜单
	const [fieldMenu, setFieldMenu] = useState<string[]>([]);

	// table 加载完成保存table中的所有item,因为最终要提交给后台,table组件没有获取
	// 所有item的属性
	const initDataSource = (items: API.FieldMappingListItem[]) => {
		// 因为 onload方法可能会调用多次,可能在没有数据的情况下被调用
		if (undefined == items || items.length <= 0) {
			return;
		}

		// 页面重新渲染重复加载,清除原有数据
		loadedTableItems.splice(0, loadedTableItems.length);

		for (let i = 0; i < items.length; i++) {
			let item: API.FieldMappingListItem = {...items[i]};
			loadedTableItems.push(item);
		}

		// 加载下拉数据
		fetchFieldMenu(loadedTableItems);
		setConnectionFieldMappings(loadedTableItems);
	}

	const editDataSource = (
		curItem: API.FieldMappingListItem,
		oldItem: API.FieldMappingListItem) => {
		if (loadedTableItems && loadedTableItems.length > 0) {
			// 找到对应数据并且进行修改,然后重新放到数组中
			loadedTableItems.forEach((record, index, arr) => {
				if (curItem.id == record.id) {
					arr[index] = curItem;
				}
			})
		}
		setConnectionFieldMappings(loadedTableItems);

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
			hideInTable: connectionFieldMappingModel.sinkDataSystemType == 'KAFKA',
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
			hideInTable: showHandleColumn,
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

	/**
		查找源数据集列表
	*/
	const fetchFieldMenu = async (itemList: API.FieldMappingListItem[]) => {
		// 源表id
		let srcDataSetId = connectionFieldMappingModel.srcDataSetId

		// 加载下拉菜单数据
		let fieldList = await fetchDataSetFields({id: srcDataSetId});
		let sourceFieldMap: Map<string, string> = new Map([]);
		itemList.forEach((val, index, arr) => {
			if (val && val.sourceFieldFormat) {
				sourceFieldMap.set(val.sourceFieldFormat, val.sourceFieldFormat);
			}
		})

		let menu: string[] | undefined = fieldList?.filter((val, index, arr) => {
			return !sourceFieldMap.get(val)
		})

		menu = !menu ? [] : menu;

		let newMenu: string[] = []

		appendMetaFieldToMenu(newMenu,UN_REUSABLE_META_FIELDS_MAP)

		// 不可重复使用的元数据填充
		menu.forEach((val, idx, arr) => {
			newMenu.push(val)
		})

		setFieldMenu(newMenu);
	}

	const appendMetaFieldToMenu = (
		newMenu: string[],
		unReusableMetaFieldMap: Map<string, string>
	) => {
		let sinkDataSystemType = connectionFieldMappingModel.sinkDataSystemType;
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

	// 事件处理保存数据
	useEffect(() => {

	});

	return (
		<EditableProTable<API.FieldMappingListItem>
			//rowKey={(record)=>{
			//console.log("rowKey record: ",record)
			//return String(record.id)}}

			// TODO 这个key 不能随便写?什么情况,使用id也是不重复的啊
			rowKey="id"
			maxLength={5}
			// 关闭默认的新建按钮
			recordCreatorProps={false}
			columns={fieldMappingColumns}
			params={{
				connectionId: connectionFieldMappingModel.connectionId,
				from: connectionFieldMappingModel.from,
				srcDataSystemType: connectionFieldMappingModel.srcDataSystemType,
				srcDataSetId: connectionFieldMappingModel.srcDataSetId,
				sinkDataSetId: connectionFieldMappingModel.sinkDataSetId,
				sinkDataSystemType: connectionFieldMappingModel.sinkDataSystemType,
			}}
			request={fetchConnectionFieldMapping}
			rowClassName={(record) => {
				if (connectionFieldMappingModel.sinkDataSystemType == 'KAFKA') {
					return
				}
				return record.sourceFieldFormat === '' ? styles['split-row-select-active'] : '';
			}}
			onLoad={(items) => {initDataSource(items);}}
			editable={{
				form,
				editableKeys,
				onSave: async (index, cur, old) => {
					if (cur && cur.filterOperator) {
						let conditionValue = cur?.filterValue
						let isMetaField = cur.sourceFieldFormat!.startsWith('__');
						// 暂时先做非空处理,后续根据情况修改正则
						let len = conditionValue?.replace(/(^\s*)|(\s*$)/g, "").length;
						let isNullExpression = (!len || len <= 0)
						let isNoSrcField = (!cur.sourceFieldFormat)

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
					editDataSource(cur, old);
				},
				onChange: setEditableRowKeys,
				actionRender: (row, config, dom) => [dom.save, dom.cancel],
			}}
		/>
	);
};

export default ConnectionFieldMappingList;
