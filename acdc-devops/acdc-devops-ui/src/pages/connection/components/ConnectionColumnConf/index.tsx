import React, { useState, useEffect, MutableRefObject, useRef } from 'react';
import type { EditableFormInstance, ProColumns } from '@ant-design/pro-table';
import { EditableProTable } from '@ant-design/pro-table';
import { Button, message, Modal, Select, Tag } from 'antd';
import { getDataCollectionDefinition } from '@/services/a-cdc/api';
import styles from './split.less';
import { ConnectionColumnConfConstant } from '@/services/a-cdc/constant/ConnectionColumnConfConstant';
import { DataSystemTypeConstant } from '@/services/a-cdc/constant/DataSystemTypeConstant';
import { EditOutlined, SwapLeftOutlined } from '@ant-design/icons';
import { isEmptyArray, isEmptyString } from '@/services/a-cdc/util/acdc-util';
import ProForm, { ProFormInstance } from '@ant-design/pro-form';
const { Option } = Select;
const { confirm } = Modal;

export type ConnectionColumnConfProps = {
  displayDataSource?: API.ConnectionColumnConf[];
  originalDataSource?: API.ConnectionColumnConf[];
  canEdit?: boolean;
  canDelete?: boolean;
  sinkDataSystemType?: string;
  sourceDataCollectionId?: number;
  sinkDataCollectionId?: number;
};

type ComponentProps = {
  columnConfProps: ConnectionColumnConfProps;
  editorFormRef?: MutableRefObject<EditableFormInstance<API.ConnectionColumnConf>>;
};

const ConnectionColumnConf: React.FC<ComponentProps> = (componentProps) => {
  let newColumnConfProps = !componentProps.columnConfProps ? {} : componentProps.columnConfProps;
  // 获取传入的属性
  const { editorFormRef } = componentProps;
  const {
    sinkDataSystemType,
    originalDataSource,
    displayDataSource,
    sourceDataCollectionId,
    canDelete,
    canEdit,
  } = newColumnConfProps;

  // 列表列字段为空的情况会展示'-',但是如果render返回null|undefined|'' 则不会展示 '-'，增加一个展示符号，与第一次渲染效果保持一致
  const NONE_TEXT: string = '-';

  // 编辑列表 row id 前缀
  const ROW_ID_PREFIX = 'row_id_';

  // 字段映射关系是匹配的排序值
  const IS_MATCH_ORDER_VALUE: number = 1;

  // 字段映射关系不匹配的排序值
  const NOT_MATCH_ORDER_VALUE: number = 2;

  // 字段索引类型为主键的排序值
  const PRIMARY_INDEX_ORDER_VALUE: number = 1;

  // 字段索引类型为唯一索引的排序值
  const UNIQUE_INDEX_ORDER_VALUE: number = 2;

  // 普通字段的排序值
  const NORMAL_FIELD_INDEX_ORDER_VALUE: number = 3;

  // ACDC 元数据字段的排序值
  const ACDC_META_FIELD_ORDER_VALUE: number = 4;

  // 空字段的排序值
  const EMPTY_FIELD_INDEX_ORDER_VALUE: number = 5;

  // 源端数据集字段下拉菜单,空值选项
  const NONE_FIELD_DEFINITION_MENU_ITEM: API.DataFieldDefinition = {
    name: '',
    type: '',
    uniqueIndexNames: [],
    displayName: NONE_TEXT,
  };

  const META_OP: API.DataFieldDefinition = {
    name: ConnectionColumnConfConstant.META_OP,
    type: ConnectionColumnConfConstant.META_OP_TYPE,
    uniqueIndexNames: [],
    displayName: ConnectionColumnConfConstant.META_OP,
  };

  const META_KAFKA_RECORD_OFFSET: API.DataFieldDefinition = {
    name: ConnectionColumnConfConstant.META_KAFKA_RECORD_OFFSET,
    type: ConnectionColumnConfConstant.META_KAFKA_RECORD_OFFSET_TYPE,
    uniqueIndexNames: [],
    displayName: ConnectionColumnConfConstant.META_KAFKA_RECORD_OFFSET,
  };

  const META_DATETIME: API.DataFieldDefinition = {
    name: ConnectionColumnConfConstant.META_DATE_TIME,
    type: ConnectionColumnConfConstant.META_DATE_TIME_TYPE,
    uniqueIndexNames: [],
    displayName: ConnectionColumnConfConstant.META_DATE_TIME,
  };

  const META_LOGIC_DEL: API.DataFieldDefinition = {
    name: ConnectionColumnConfConstant.META_LOGICAL_DEL,
    type: ConnectionColumnConfConstant.META_LOGICAL_DEL_TYPE,
    uniqueIndexNames: [],
    displayName: ConnectionColumnConfConstant.META_LOGICAL_DEL,
  };

  const ACDC_META_FIELD_NAME_MAP: Map<string, string> = new Map([
    [ConnectionColumnConfConstant.META_OP, ConnectionColumnConfConstant.META_OP],
    [
      ConnectionColumnConfConstant.META_KAFKA_RECORD_OFFSET,
      ConnectionColumnConfConstant.META_KAFKA_RECORD_OFFSET,
    ],
    [ConnectionColumnConfConstant.META_DATE_TIME, ConnectionColumnConfConstant.META_DATE_TIME],
    [ConnectionColumnConfConstant.META_LOGICAL_DEL, ConnectionColumnConfConstant.META_LOGICAL_DEL],
  ]);

  const HIVE_META_FIELD_MAP: Map<string, API.DataFieldDefinition> = new Map([
    [NONE_FIELD_DEFINITION_MENU_ITEM.displayName!, NONE_FIELD_DEFINITION_MENU_ITEM!],
    [META_OP.displayName!, META_OP!],
    [META_KAFKA_RECORD_OFFSET.displayName!, META_KAFKA_RECORD_OFFSET!],
    [META_DATETIME.displayName!, META_DATETIME!],
  ]);

  // ES 不支持ACDC元数据
  const ES_META_FIELD_MAP: Map<string, API.DataFieldDefinition> = new Map([
    [NONE_FIELD_DEFINITION_MENU_ITEM.displayName!, NONE_FIELD_DEFINITION_MENU_ITEM!],
  ]);

  const STARROCKS_META_FIELD_MAP: Map<string, API.DataFieldDefinition> = new Map([
    [NONE_FIELD_DEFINITION_MENU_ITEM.displayName!, NONE_FIELD_DEFINITION_MENU_ITEM!],
  ]);

  const JDBC_META_FIELD_MAP: Map<string, API.DataFieldDefinition> = new Map([
    [NONE_FIELD_DEFINITION_MENU_ITEM.displayName!, NONE_FIELD_DEFINITION_MENU_ITEM!],
    [META_OP.displayName!, META_OP!],
    [META_KAFKA_RECORD_OFFSET.displayName!, META_KAFKA_RECORD_OFFSET!],
    [META_DATETIME.displayName!, META_DATETIME!],
    [META_LOGIC_DEL.displayName!, META_LOGIC_DEL!],
  ]);

  // 主键类型
  const PRIMARY_INDEX_NAME_MAP: Map<string, string> = new Map([['PRIMARY', 'PRIMARY']]);

  // 表格展示数据源状态
  const [tableDisplayDataSourceState, setTableDisplayDataSourceState] = useState<
    API.ConnectionColumnConf[]
  >([]);

  // 表格原始数据源状态
  const [tableOriginalDataSourceState, setTableOriginalDataSourceState] = useState<
    API.ConnectionColumnConf[]
  >([]);

  // 加载遮罩
  const [loadingState, setLoadingState] = useState(true);

  // 编辑表格启动编辑模式,固定的state触发
  const [editableKeys, setEditableRowKeys] = useState<React.Key[]>(() => []);

  // 源端数据集字段下拉菜单
  const [srcDataCollectionFieldMenuState, setSrcDataCollectionFieldMenuState] = useState<
    API.DataFieldDefinition[]
  >([]);

  // 源端数据集字段下拉菜单 item 元素
  const [srcFieldDefinitionMenuItemMappingState, setSrcFieldDefinitionMenuItemMappingState] =
    useState<Map<String, API.DataFieldDefinition>>(new Map());

  const [refreshVersionState, setRefreshVersionState] = useState<number>(-99);

  const formRef = useRef<ProFormInstance<any>>();

  /**
   * 初始化组件数据, 初始化相关状态
   */
  useEffect(() => {
    if (checkComponentProps()) {
      initState();
      setLoadingState(false);
    }
  }, [originalDataSource, displayDataSource]); // 数据源如果没有变化，认为列表状态数据不需要更新

  /**
   * 组件传入的参数校验
   */
  const checkComponentProps = () => {
    if (
      !displayDataSource ||
      !Array.isArray(displayDataSource) ||
      !originalDataSource ||
      !Array.isArray(originalDataSource) ||
      !sinkDataSystemType ||
      canEdit == undefined ||
      canDelete == undefined
    ) {
      console.log(
        '必传参数: [displayDataSource,originalDataSource,sinkDataSystemType,canEdit,canDelete]',
      );
      return false;
    }

    if (canEdit && !sourceDataCollectionId) {
      console.log('当前组件设置为可编辑模式,必传参数: [sourceDataCollectionId]');
      return false;
    }
    return true;
  };

  /**
   * 字段配置列表排序
   */
  const sortProTableDataSource = (columnConfs: API.ConnectionColumnConf[]) => {
    columnConfs.sort((n1, n2) => {
      // n1.localeCompare(n2,'zh-CN')
      const n1SortSequence = generateColumnConfSortSequence(n1);
      const n2SortSequence = generateColumnConfSortSequence(n2);
      return n1SortSequence.localeCompare(n2SortSequence);
    });
  };

  /**
   *  生成 connection 列配置 record 排序字符串序列名称
   */
  const generateColumnConfSortSequence = (columnConf: API.ConnectionColumnConf) => {
    const columnConfFieldMatchingRate: number = calculateColumnConfFieldMatchingRate(columnConf);

    const columnConfOrderValue: number = calculateColumnConfOrderValue(columnConf);

    return '' + columnConfFieldMatchingRate + columnConfOrderValue + columnConf.sourceColumnName;
  };

  /**
   *  计算 column 源端字段和目标端字段的字段匹配率
   */
  const calculateColumnConfFieldMatchingRate = (columnConf: API.ConnectionColumnConf) => {
    const sourceColumnName = columnConf.sourceColumnName;
    const sinkColumnName = columnConf.sinkColumnName;

    return sourceColumnName === sinkColumnName ? IS_MATCH_ORDER_VALUE : NOT_MATCH_ORDER_VALUE;
  };

  /**
   * 计算 column 排序值
   */
  const calculateColumnConfOrderValue = (columnConf: API.ConnectionColumnConf) => {
    const sourceColumnUniqueIndexNames: string[] | undefined =
      columnConf.sourceColumnUniqueIndexNames;
    const sourceColumnName: string = columnConf.sourceColumnName!;
    // empty field
    if (isEmptyString(sourceColumnName)) {
      return EMPTY_FIELD_INDEX_ORDER_VALUE;
    }

    // acdc meta field
    if (ACDC_META_FIELD_NAME_MAP.has(sourceColumnName)) {
      return ACDC_META_FIELD_ORDER_VALUE;
    }

    // normal field
    if (isEmptyArray(sourceColumnUniqueIndexNames)) {
      return NORMAL_FIELD_INDEX_ORDER_VALUE;
    }

    // primary index field
    for (let uniqueIndexName of sourceColumnUniqueIndexNames!) {
      if (PRIMARY_INDEX_NAME_MAP.has(uniqueIndexName)) {
        return PRIMARY_INDEX_ORDER_VALUE;
      }
    }

    // unique index field
    return UNIQUE_INDEX_ORDER_VALUE;
  };

  /**
   *  把数据源转换为 ProTable 组件所需要的数据源
   *
   *  1. 由于组件对rowid处理的限制，rowId不能以0 或者1开头，否则可能导致编辑出现多行编辑的问题
   *  2. rowId 的生成规则为 数组的 固定前缀+当前元素所在数组的索引位置+1
   */
  const newProTableDataSource = (dataSource: API.ConnectionColumnConf[]) => {
    const proTableDisplayDataSource: API.ConnectionColumnConf[] = [];

    dataSource.forEach((element, index, _arr) => {
      const rowIndex = index + 1;
      const rowId = ROW_ID_PREFIX + rowIndex;

      const id = element.id!;

      const sourceColumnName = isEmptyString(element.sourceColumnName)
        ? ''
        : element.sourceColumnName;

      const sourceColumnType = isEmptyString(element.sourceColumnType)
        ? ''
        : element.sourceColumnType;

      const sourceColumnUniqueIndexNames = isEmptyArray(element.sourceColumnUniqueIndexNames)
        ? []
        : element.sourceColumnUniqueIndexNames;

      const sinkColumnName = isEmptyString(element.sinkColumnName) ? '' : element.sinkColumnName;

      const sinkColumnType = isEmptyString(element.sinkColumnType) ? '' : element.sinkColumnType;

      const sinkColumnUniqueIndexNames = isEmptyArray(element.sinkColumnUniqueIndexNames)
        ? []
        : element.sinkColumnUniqueIndexNames;

      const filterOperator = isEmptyString(element.filterOperator) ? '' : element.filterOperator;

      const filterValue = isEmptyString(element.filterValue) ? '' : element.filterValue;

      const newColumnConf = {
        id,
        rowId,
        sourceColumnName,
        sourceColumnType,
        sourceColumnUniqueIndexNames,
        sinkColumnName,
        sinkColumnType,
        sinkColumnUniqueIndexNames,
        filterOperator,
        filterValue,
      };

      proTableDisplayDataSource.push(newColumnConf);
    });

    return proTableDisplayDataSource;
  };

  /**
   * 初始化源端字段下拉数据
   */
  const initSrcDataCollectionFieldMenuState = (
    tableDisplayDataSource: API.ConnectionColumnConf[],
    dataCollectionId: number,
  ) => {
    if (!dataCollectionId) {
      return;
    }
    doInitSrcDataCollectionFieldMenuState(tableDisplayDataSource, dataCollectionId);
  };

  /**
   * 初始化状态数据
   */
  const initState = () => {
    // 展示数据源
    let tableDisplayDataSource: API.ConnectionColumnConf[] = newProTableDataSource(
      displayDataSource!,
    );
    sortProTableDataSource(tableDisplayDataSource);

    // 原始数据源
    let tableOriginalDataSource: API.ConnectionColumnConf[] = newProTableDataSource(
      originalDataSource!,
    );
    sortProTableDataSource(tableOriginalDataSource);

    // 保存表格的原始数据源,和展示数据源到状态中
    setTableDisplayDataSourceState(tableDisplayDataSource);
    setTableOriginalDataSourceState(tableOriginalDataSource);

    // 初始化下拉菜单数据
    if (canEdit) {
      initSrcDataCollectionFieldMenuState(tableDisplayDataSource, sourceDataCollectionId!);
    }

    setRefreshVersionState(refreshVersionState + 1);
  };

  /**
    重新加载可编辑表格数据源.
    刷新,初始化时会调用
  */
  const reload = () => {
    let tableDisplayDataSource: API.ConnectionColumnConf[] = newProTableDataSource(
      tableOriginalDataSourceState,
    );
    setTableDisplayDataSourceState(tableDisplayDataSource);
    setRefreshVersionState(refreshVersionState + 1);
  };

  /**
    创建源端数据集字段下拉菜单选项数据
  */
  const toFieldDefinitionMenuRecord = (fieldDefinition: API.DataFieldDefinition) => {
    let name = fieldDefinition.name!;
    let type = fieldDefinition.type!;
    let uniqueIndexNames = isEmptyArray(fieldDefinition.uniqueIndexNames)
      ? []
      : fieldDefinition.uniqueIndexNames;
    let displayName = fieldDefinition.name!;
    return {
      name,
      type,
      uniqueIndexNames,
      displayName,
    };
  };

  /**

    更新可编辑表格的展示数据
    1. onSave 会调用,用表格最新数据更新数据源对应的数据行
    2. onDelete 会调用,删除数据源的对应的数据行
  */
  const updateInTableDisplayDataSource = (curRecord: API.ConnectionColumnConf) => {
    let tableDisplayDataSource: API.ConnectionColumnConf[] = [];
    tableDisplayDataSourceState.forEach((record, _index, _arr) => {
      tableDisplayDataSource.push(record);
    });

    let sourceFieldDefinition = getSrcFieldDefinitionMenuItemBySourceColumnName(
      curRecord.sourceColumnName,
    );
    let sourceColumnName = sourceFieldDefinition.name;
    let sourceColumnType = sourceFieldDefinition.type;
    let sourceColumnUniqueIndexNames = sourceFieldDefinition.uniqueIndexNames;
    // update current item
    tableDisplayDataSource.forEach((element, index, arr) => {
      if (element.id === curRecord.id) {
        arr[index] = {
          ...curRecord,
          sourceColumnName,
          sourceColumnType,
          sourceColumnUniqueIndexNames,
        };
      }
    });

    // update state
    setTableDisplayDataSourceState(tableDisplayDataSource);
    setRefreshVersionState(refreshVersionState + 1);
  };

  /**
   * 生成唯一索引的展示文本
   */
  const generateUniqueIndexNamesText = (uniqueIndexNames?: string[]) => {
    let uniqueIndexNamesText = uniqueIndexNamesTextOf(uniqueIndexNames);
    // 索引标签
    return (
      <>
        {uniqueKeyTagOf(uniqueIndexNames)}&nbsp;{uniqueIndexNamesText}
      </>
    );
  };

  /**
   * 获取唯一索引的展示文本
   */
  const uniqueIndexNamesTextOf = (uniqueIndexNames?: string[]) => {
    if (!uniqueIndexNames || uniqueIndexNames.length < 1) {
      return NONE_TEXT;
    }

    let newUniqueIndexNames: string[] = [];
    uniqueIndexNames.forEach((record, _index, _arr) => {
      newUniqueIndexNames.push(record);
    });

    newUniqueIndexNames.sort((n1, n2) => {
      return n1.length - n2.length;
    });

    // 生成索引名称
    let uniqueIndexNamesText = '';
    uniqueIndexNames!.forEach((it, index, _arr) => {
      if (index == 0) {
        uniqueIndexNamesText = it;
        return;
      }
      uniqueIndexNamesText = uniqueIndexNamesText + ',' + it;
    });

    return uniqueIndexNamesText;
  };

  /**
   * 获取唯一索引的标签
   * 如果字段不存在唯一索引,则返回空标签
   */
  const uniqueKeyTagOf = (uniqueIndexNames?: string[]) => {
    let uniqueIndexNamesText = uniqueIndexNamesTextOf(uniqueIndexNames);
    if (!uniqueIndexNamesText || uniqueIndexNamesText == NONE_TEXT) {
      return '';
    }
    for (let entity of PRIMARY_INDEX_NAME_MAP.entries()) {
      var myRe = new RegExp('.*' + entity[0] + '.*', 'g');
      if (myRe.test(uniqueIndexNamesText)) {
        return <Tag color={'red'}>{'PK'}</Tag>;
      }
    }
    return <Tag color={'green'}>{'UK'}</Tag>;
  };

  const generateFieldNameTextWithTag = (fieldName?: string) => {
    if (isEmptyString(fieldName)) {
      return NONE_TEXT;
    }
    return (
      <>
        {fieldName}&nbsp;{metaFieldTagOf(fieldName)}
      </>
    );
  };

  /**
   * acdc 元数据标签生成
   */
  const metaFieldTagOf = (fieldName?: string) => {
    if (isEmptyString(fieldName) || !ACDC_META_FIELD_NAME_MAP.has(fieldName!)) {
      return '';
    }
    return (
      <>
        {' '}
        <Tag color={'cyan'}>{'ACDC'}</Tag>
      </>
    );
  };

  /**
   * 删除可编辑表格数据源的数据行
   *
   */
  const deleteInTableDisplayDataSource = (curRecord: API.ConnectionColumnConf) => {
    let tableDisplayDataSource: API.ConnectionColumnConf[] = [];
    tableDisplayDataSourceState.forEach((element, _index, _arr) => {
      if (element.rowId !== curRecord.rowId) {
        tableDisplayDataSource.push(element);
      }
    });

    // update state
    setTableDisplayDataSourceState(tableDisplayDataSource);
    setRefreshVersionState(refreshVersionState + 1);
  };

  /**
   *
   * 根据字段配置的源端字段名获取对应的下拉菜单选项
   *
   * 1. 如果sourceColumnName 为空,则会匹配到空值选项
   */
  const getSrcFieldDefinitionMenuItemBySourceColumnName = (sourceColumnName?: string) => {
    if (isEmptyString(sourceColumnName)) {
      return NONE_FIELD_DEFINITION_MENU_ITEM;
    }

    return srcFieldDefinitionMenuItemMappingState.get(sourceColumnName!)!;
  };

  const doSave = (curRecord: API.ConnectionColumnConf, oldRecord: API.ConnectionColumnConf) => {
    // 1. 对下拉菜单选中数据项,做释放操作
    // 2. 逻辑删除元数据字段需要做为普通字段处理
    // 3. 可以重复使用的原字段一直保持排序在最顶端
    //let newMenu: API.DataFieldDefinition[] = [];

    let tempMenu = srcDataCollectionFieldMenuState;
    let tempMenuMapping = new Map<String, API.DataFieldDefinition>();

    tempMenu.forEach((val, _index, _arr) => {
      tempMenuMapping.set(val.displayName!, val);
    });

    let curSourceColumnDisplayName = getSrcFieldDefinitionMenuItemBySourceColumnName(
      curRecord.sourceColumnName,
    ).displayName!;
    let oldSourceColumnDisplayName = getSrcFieldDefinitionMenuItemBySourceColumnName(
      oldRecord.sourceColumnName,
    ).displayName!;

    tempMenuMapping.set(
      oldSourceColumnDisplayName!,
      srcFieldDefinitionMenuItemMappingState.get(oldSourceColumnDisplayName!)!,
    );

    if (sinkDataSystemType == DataSystemTypeConstant.HIVE) {
      if (!HIVE_META_FIELD_MAP.has(curSourceColumnDisplayName!)) {
        tempMenuMapping.delete(curSourceColumnDisplayName);
      }
    }

    if (sinkDataSystemType == DataSystemTypeConstant.ELASTICSEARCH) {
      if (!ES_META_FIELD_MAP.has(curSourceColumnDisplayName!)) {
        tempMenuMapping.delete(curSourceColumnDisplayName);
      }
    }

    if (sinkDataSystemType == DataSystemTypeConstant.STARROCKS) {
      if (!STARROCKS_META_FIELD_MAP.has(curSourceColumnDisplayName!)) {
        tempMenuMapping.delete(curSourceColumnDisplayName);
      }
    }


    if (
      sinkDataSystemType == DataSystemTypeConstant.TIDB ||
      sinkDataSystemType == DataSystemTypeConstant.MYSQL
    ) {
      if (curSourceColumnDisplayName == META_LOGIC_DEL.displayName) {
        tempMenuMapping.delete(curSourceColumnDisplayName!);
      } else if (!JDBC_META_FIELD_MAP.has(curSourceColumnDisplayName!)) {
        tempMenuMapping.delete(curSourceColumnDisplayName!);
      }
    }

    let newMenu = [];
    for (let entity of tempMenuMapping.entries()) {
      newMenu.push(entity[1]);
    }

    // 更新源端字段下拉菜单
    sortSrcDataCollectionFieldMenu(newMenu);
    setSrcDataCollectionFieldMenuState(newMenu);
    setRefreshVersionState(refreshVersionState + 1);

    updateInTableDisplayDataSource(curRecord);
  };

  const onSave = (curItem: API.ConnectionColumnConf, oldItem: API.ConnectionColumnConf) => {
    if (curItem && curItem.filterOperator) {
      let conditionValue = curItem?.filterValue;
      let isMetaField = curItem.sourceColumnName!.startsWith(
        ConnectionColumnConfConstant.META_PREFIX,
      );

      // 暂时先做非空处理,后续根据情况修改正则
      let len = conditionValue?.replace(/(^\s*)|(\s*$)/g, '').length;
      let isNullExpression = !len || len <= 0;
      let isNoSrcField = !curItem.sourceColumnName;

      if (isMetaField || isNullExpression || isNoSrcField) {
        curItem.filterValue = '';
        curItem.filterOperator = '';
      }

      // message
      if (isMetaField) {
        message.warn('"ACDC 字段",不能配置列过滤条件');
      } else if (isNoSrcField) {
        message.warn('配置列过滤条件,源字段不能为空');
      } else if (isNullExpression) {
        message.warn('配置列过滤条件,过滤值不能为空');
      } else {
        // pass
      }
    }
    doSave(curItem, oldItem);
  };

  const onDelete = (curRecord: API.ConnectionColumnConf) => {
    confirm({
      title: '确定删除吗',
      icon: <EditOutlined />,
      content: '删除',
      async onOk() {
        doDelete(curRecord);
      },
      onCancel() {},
    });
  };

  const doDelete = (curRecord: API.ConnectionColumnConf) => {
    deleteInTableDisplayDataSource(curRecord);
  };

  /**
   * 初始化源端字段下拉菜单
   */
  const doInitSrcDataCollectionFieldMenuState = async (
    tableDisplayDataSource: API.ConnectionColumnConf[],
    dataCollectionId: number,
  ) => {
    // 请求下拉菜单数据
    let definition = (await getDataCollectionDefinition({
      id: dataCollectionId,
    })) as API.DataCollectionDefinition;
    let { lowerCaseNameToDataFieldDefinitions } = definition;

    // 所有字段声明 mapping
    let srcFieldDefinitionMenuItemMapping: Map<String, API.DataFieldDefinition> = new Map();

    // 下拉菜单 mapping
    let srcDataCollectionFieldMenuMapping: Map<String, API.DataFieldDefinition> = new Map();
    for (let key in lowerCaseNameToDataFieldDefinitions) {
      let newItem = toFieldDefinitionMenuRecord(lowerCaseNameToDataFieldDefinitions[key]);
      srcFieldDefinitionMenuItemMapping.set(newItem.displayName, newItem);
      srcDataCollectionFieldMenuMapping.set(newItem.displayName, newItem);
    }

    // 移除匹配到的字段
    let srcDataCollectionFieldMenu: API.DataFieldDefinition[] = [];
    for (let currentColumnConfig of tableDisplayDataSource) {
      if (!isEmptyString(currentColumnConfig.sourceColumnName)) {
        srcDataCollectionFieldMenuMapping.delete(currentColumnConfig.sourceColumnName!);
      }
    }

    // 添加元数据字段
    appendMetaFiled(srcFieldDefinitionMenuItemMapping);
    appendMetaFiled(srcDataCollectionFieldMenuMapping);

    // 转换为源端下拉菜单数据结构
    for (let entity of srcDataCollectionFieldMenuMapping.entries()) {
      srcDataCollectionFieldMenu.push(entity[1]);
    }

    // 源端字段下拉菜单排序
    sortSrcDataCollectionFieldMenu(srcDataCollectionFieldMenu);

    // 保存源端字段下拉菜单,状态数据
    setSrcDataCollectionFieldMenuState(srcDataCollectionFieldMenu);

    // 保存源端字段下拉菜单 item 状态数据
    setSrcFieldDefinitionMenuItemMappingState(srcFieldDefinitionMenuItemMapping);
  };

  /**
   * 添加元数据字段,到当前的源端字段下拉菜单中
   */
  const appendMetaFiled = (srcDataFiledDefinitionMapping: Map<String, API.DataFieldDefinition>) => {
    // 元数据增加
    if (sinkDataSystemType == DataSystemTypeConstant.HIVE) {
      for (let entity of HIVE_META_FIELD_MAP.entries()) {
        srcDataFiledDefinitionMapping.set(entity[0]!, entity[1]);
      }
    }

    if (sinkDataSystemType == DataSystemTypeConstant.ELASTICSEARCH) {
      for (let entity of ES_META_FIELD_MAP.entries()) {
        srcDataFiledDefinitionMapping.set(entity[0]!, entity[1]);
      }
    }

    if (sinkDataSystemType == DataSystemTypeConstant.STARROCKS) {
      for (let entity of STARROCKS_META_FIELD_MAP.entries()) {
        srcDataFiledDefinitionMapping.set(entity[0]!, entity[1]);
      }
    }

    if (
      sinkDataSystemType == DataSystemTypeConstant.TIDB ||
      sinkDataSystemType == DataSystemTypeConstant.MYSQL
    ) {
      for (let entity of JDBC_META_FIELD_MAP.entries()) {
        srcDataFiledDefinitionMapping.set(entity[0]!, entity[1]);
      }
    }
  };

  /**
   *  源端字段下拉菜单字段排序
   *
   *  1. 元数据字段排在最顶端
   *  2. 普通字段按照名称排序
   */
  const sortSrcDataCollectionFieldMenu = (
    currentSrcDataCollectionFieldMenu: API.DataFieldDefinition[],
  ) => {
    return currentSrcDataCollectionFieldMenu.sort((n1, n2) => {
      return (
        calculateFieldDefinitionMenuItemOrderValue(n1) -
        calculateFieldDefinitionMenuItemOrderValue(n2)
      );
    });
  };

  /**
   * 源端字段下拉菜单选项的排序值
   */
  const calculateFieldDefinitionMenuItemOrderValue = (
    currentFieldDefinition: API.DataFieldDefinition,
  ) => {
    let defaultSortValue = 100;

    if (!currentFieldDefinition) {
      return defaultSortValue;
    }
    // 空值选项
    if (currentFieldDefinition.name == NONE_FIELD_DEFINITION_MENU_ITEM.name) {
      return 1;
    }

    // 元数据字段
    if (currentFieldDefinition.name == META_OP.name) {
      return 2;
    }
    if (currentFieldDefinition.name == META_KAFKA_RECORD_OFFSET.name) {
      return 3;
    }
    if (currentFieldDefinition.name == META_DATETIME.name) {
      return 4;
    }
    if (currentFieldDefinition.name == META_LOGIC_DEL.name) {
      return 5;
    }

    // 普通字段
    return defaultSortValue + currentFieldDefinition.name!.length;
  };

  const columns: ProColumns<API.ConnectionColumnConf>[] = [
    {
      title: '目标字段',
      dataIndex: 'sinkColumnName',
      width: '10%',
      editable: false,
      render: (_text, record, _, _action) => [generateFieldNameTextWithTag(record.sinkColumnName)],
    },
    {
      title: '源字段',
      dataIndex: 'sourceColumnName',
      valueType: 'select',
      hideInTable: sinkDataSystemType == DataSystemTypeConstant.KAFKA,
      width: '20%',
      render: (_text, record, _, _action) => [
        generateFieldNameTextWithTag(record.sourceColumnName),
      ],
      renderFormItem: (_record) => {
        return (
          <Select
            showSearch
            showArrow
            placeholder="请选择"
            style={{ minWidth: 200 }}
            optionFilterProp="children"
            onSelect={(_item) => {}}
            virtual={true}
          >
            {srcDataCollectionFieldMenuState.map((item) => {
              return (
                <Option value={item.name} key={item.name}>
                  {item.displayName}&nbsp;&nbsp;{item.type}&nbsp;&nbsp;
                  {uniqueKeyTagOf(item.uniqueIndexNames)}&nbsp;&nbsp;{metaFieldTagOf(item.name)}
                </Option>
              );
            })}
          </Select>
        );
      },
    },
    {
      title: '目标字段类型',
      dataIndex: 'sinkColumnType',
      width: '7%',
      editable: false,
    },
    {
      title: '源字段类型',
      dataIndex: 'sourceColumnType',
      width: '7%',
      editable: false,
    },
    {
      title: '目标字段唯一索引',
      dataIndex: 'sinkColumnUniqueIndexNames',
      width: '15%',
      editable: false,
      render: (_text, record, _, _action) => [
        generateUniqueIndexNamesText(record.sinkColumnUniqueIndexNames),
      ],
    },
    {
      title: '源字段唯一索引',
      dataIndex: 'sourceColumnUniqueIndexNames',
      width: '15%',
      editable: false,
      render: (_text, record, _, _action) => [
        generateUniqueIndexNamesText(record.sourceColumnUniqueIndexNames),
      ],
    },

    {
      title: '过滤条件',
      dataIndex: 'filterOperator',
      valueType: 'select',
      width: '10%',
      valueEnum: {
        '<': { text: '<', value: '<' },
        '<=': { text: '<=', value: '<=' },
        '==': { text: '==', value: '==' },
        '!=': { text: '!=', value: '!=' },
        '>=': { text: '>=', value: '>=' },
        '>': { text: '>', value: '>' },
      },
    },
    {
      title: '过滤值',
      width: '10%',
      dataIndex: 'filterValue',
    },
    {
      title: '操作',
      hideInTable: !canEdit && !canDelete,
      width: '6%',
      valueType: 'option',
      render: (_text, record, _, action) => [
        <div>
          {canEdit ? (
            <a
              key="editable"
              onClick={() => {
                if (record.rowId) {
                  action?.startEditable?.(record.rowId);
                }
              }}
            >
              编辑
            </a>
          ) : (
            <></>
          )}
        </div>,
        <div>
          {canDelete ? (
            <a
              key={'delete'}
              onClick={() => {
                onDelete(record);
              }}
            >
              删除
            </a>
          ) : (
            <></>
          )}
        </div>,
      ],
    },
  ];

  return (
    <ProForm<{
      table: API.ConnectionColumnConf[];
    }>
      formRef={formRef}
      //initialValues={{
      //table: dataSource,
      //}}
      //
      submitter={{
        // 隐藏重置和提交按钮
        render: (_props, _doms) => {
          return [];
        },
      }}
      validateTrigger="onBlur"
    >
      <EditableProTable<API.ConnectionColumnConf>
        //这个rowKey必须为表格的record对象中的字段名称,不能自己生成
        // eg: rowKey='rowId'
        //rowKey={(record) => String(record.id)}
        rowKey="rowId"
        scroll={{
          x: 960,
        }}
        name="table"
        editableFormRef={editorFormRef}
        maxLength={50}
        recordCreatorProps={false}
        columns={columns}
        loading={loadingState}
        params={{
          refreshVersion: refreshVersionState,
        }}
        options={{
          setting: {
            listsHeight: 400,
          },
          reload: false,
        }}
        request={async () => {
          return {
            success: true,
            data: tableDisplayDataSourceState,
          };
        }}
        rowClassName={(record) => {
          if (sinkDataSystemType == DataSystemTypeConstant.KAFKA) {
            return;
          }

          if (!canEdit && !canDelete) {
            return;
          }
          return isEmptyString(record.sourceColumnName) ? styles['split-row-select-active'] : '';
        }}
        onLoad={(_records) => {
          // do nothing
        }}
        toolBarRender={() => [
          <Button.Group key="refs" style={{ display: 'block' }}>
            <Button
              key="button"
              icon={<SwapLeftOutlined />}
              hidden={!canDelete && !canEdit}
              onClick={() => {
                reload();
              }}
            >
              还原修改
            </Button>
          </Button.Group>,
        ]}
        editable={{
          type: 'single',
          editableKeys,
          onChange: setEditableRowKeys,
          actionRender: (_row, _config, dom) => [dom.save, dom.cancel],
          onSave: async (index, cur, _old) => {
            let arrIndex: number = index as number;
            let curRecord = { ...cur };
            let oldRecord: API.ConnectionColumnConf = { ...tableDisplayDataSourceState[arrIndex] };
            onSave(curRecord, oldRecord);
          },
          onDelete: async (_key, record) => {
            onDelete(record);
          },
        }}
      />
    </ProForm>
  );
};

export default ConnectionColumnConf;
