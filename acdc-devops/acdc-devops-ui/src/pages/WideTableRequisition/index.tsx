import { useEffect, useRef, useState } from 'react';

// import { Scrollbars } from 'react-custom-scrollbars';
// https://github.com/malte-wessel/react-custom-scrollbars
/**
 *
  <Scrollbars
  autoHide
  autoHideTimeout={1000}
  autoHideDuration={200}
>
</Scrollbars>
 *
 * */

// https://github.com/securingsincity/react-ace
import AceEditor from 'react-ace';
import 'ace-builds/src-noconflict/ext-language_tools';
import 'ace-builds/src-noconflict/mode-mysql';
import 'ace-builds/src-noconflict/theme-monokai';
import { Ace } from 'ace-builds';
// https://github.com/sql-formatter-org/sql-formatter
import { format } from 'sql-formatter';
import {
  Breadcrumb,
  Button,
  Drawer,
  Input,
  message,
  Select,
  Space,
  Table,
  Typography,
  Form,
  Modal,
  Empty,
} from 'antd';
import { history } from 'umi';
import { Route } from 'antd/lib/breadcrumb/Breadcrumb';
import DcSearcher, { B_BNAME_SEPARATOR, SearchRecord } from '../connection/components/DcSearcher';
import { DataSystemTypeConstant } from '@/services/a-cdc/constant/DataSystemTypeConstant';
import { DataSystemResourceTypeConstant } from '@/services/a-cdc/constant/DataSystemResourceTypeConstant';
import { ProCard } from '@ant-design/pro-card';
import { createWideTable, pagedQueryProject } from '@/services/a-cdc/api';
import SqColLineageDiagram, { SqColBeautifier } from '@/components/widetable/SqColLineageDiagram';
import { CloseOutlined, DeleteOutlined, PlusOutlined, ScissorOutlined } from '@ant-design/icons';
import { StepsForm } from '@ant-design/pro-form';
import { SearchScope } from '@/services/a-cdc/constant/SearchScope';
import { AcdcCollections } from '@/services/a-cdc/util/collection-util';
import styles from './index.less';

// 解决自定义渲染 Select 组件报错问题
const { Option } = Select;
const { confirm } = Modal;

/**
 * 项目
 */
type Project = {
  id: number;
  name: string;
  owner?: string;
};

/**
 * 数据集记录
 **/
type DcRecord = {
  key: string;
  id: number;
  name: string;
  routes: Route[];
  chosenPrj?: Project;
  chosenPrjTitle?: string;
  projects: Project[];
};

/**
 * 可编辑单元格属性
 */
interface EditableCellProps extends React.HTMLAttributes<HTMLElement> {
  dataIndex: string;
  title: any;
  record: DcRecord;
  index: number;
  // react 父容器可以把子组件源端，div 等 默认传递给父组件，这个是react框架完成的,非antd的属性
  children: React.ReactNode;
}

const WideTableRequisition: React.FC = () => {
  const EMPTY_STRING = '';

  // 滚动条动态计算
  const S_SCROLL = {
    y: '23vh',
  };

  // 展示 SQL
  const DISPLAY_SQL =
    '-- ******************************************************************************************** \n' +
    '-- 1. 在编写SQL前，请先添加数据集\n' +
    '-- 2. 在选择完数据集之后，需要指定源端的数据负责人，在宽表创建完成后，负责人会进行数据审批\n' +
    '-- 3. 在ACDC中所有数据集名称都会增加一个唯一ID的后缀\n' +
    '-- 4. 宽表的主键选择，需要保证唯一性\n' +
    '-- ******************************************************************************************** \n' +
    'select\n' +
    '  c.name as cityName,\n' +
    '  u.name as userName\n' +
    'from\n' +
    '  city_23270 c\n' +
    '  left join user_23381 u on c.id = u.city_id\n' +
    'where\n' +
    '  c.id = 100\n' +
    "  and c.name = '北京'";

  // 空格
  const BLANK: string = ' ';

  // ACE 组件编辑错误警告次数限制
  const ACE_VALUE_EDIT_WARN_COUNT_LIMIT = 10;

  // 数据集搜索组件抽屉显示状态
  const [dcSearcherDwOpenFlag, setDcSearcherDwOpenFlag] = useState<boolean>(false);

  // 页面刷新版本号
  const [loadV, setLoadV] = useState<number>(-99);

  // 数据集记录
  const [dcRecords, setDcRecords] = useState<DcRecord[]>([]);

  // 宽表子查询对象
  const [subQuery, setSubQuery] = useState<API.WideTableSubquery>();

  // 宽表申请，分步表单当前步骤
  const [curStep, setCurStep] = useState<number>(0);

  // 用户项目 Map
  const [userProjectMap, setUserProjectMap] = useState<Map<number, API.Project>>(new Map());

  // 被选中的用户项目
  const [chosenUserPrj, setChosenUserPrj] = useState<API.Project>();

  // 宽表主键 Map
  const [wideTablePkMap, setWideTablePkMap] = useState<Map<number, API.WideTableSubqueryColumn>>(
    new Map(),
  );

  // 被选择的宽表主键
  const [chosenWideTablePkMap] = useState<Map<number, API.WideTableSubqueryColumn>>(new Map());

  // SQL校验错误信息组件抽屉显示状态
  const [sqlErrorMsgDwOpenFlag, setSqlErrorMsgDwOpenFlag] = useState<boolean>(false);

  // SQL 校验错误信息
  const [sqlErrorMsg, setSqlErrorMsg] = useState<string>('');

  // ACE 组件 value
  const [aceValue, setAceValue] = useState<string>('');

  // ACE 组件初始化提示词
  const [aceInitCompleters, setAceInitCompleters] = useState<Ace.Completer[]>([]);

  // ACE 组件编辑状态
  const [aceValueEditing, setAceValueEditing] = useState<boolean>(false);

  // ACE 组件编辑错误警告次数
  const [aceValueEditWarnCount, setAceValueEditWarnCount] = useState<number>(0);

  // SQl 中使用的到的数据集的 key
  const [sqlUsedRowKeys, setSqlUsedRowKeys] = useState<React.Key[]>([]);

  // 数据集选中的 key
  const [selectedRowKeys, setSelectedRowKeys] = useState<React.Key[]>([]);

  // 数据集表格隐藏控制
  const [dcTableHiddenFlag, setDcTableHiddenFlag] = useState<boolean>(true);

  // 数据集多选设置
  const rowSelection = {
    selectedRowKeys,
    onChange: (newSelectedRowKeys: React.Key[]) => {
      setSelectedRowKeys(newSelectedRowKeys);
    },
  };

  // 表单 step3 form ref
  const [step3Form] = Form.useForm();

  // 数据集表格 form ref
  const [dcTableForm] = Form.useForm();

  // ACE editor ref
  const aceEditorRef = useRef<AceEditor | null>(null);

  // modal
  //const warning = () => {
  //  Modal.warning({
  //    title: '提示',
  //    content: '请选择数据集',
  //  });
  //};

  /**
   * 查询当前用户所属项目.
   *
   * @returns 项目列表
   **/
  const queryProjectByCurrentUser = async () => {
    const projectQuery = {
      scope: SearchScope.CURRENT_USER,
      deleted: false,
      current: 1,
      pageSize: 100,
    };

    const pageProject = await pagedQueryProject(projectQuery);
    const projects = pageProject?.data;
    return projects ? projects : [];
  };

  const wideTablePkOnDeselect = (id: number): void => {
    chosenWideTablePkMap.delete(id);
  };

  const wideTablePkOnSelect = (id: number): void => {
    chosenWideTablePkMap.set(id, wideTablePkMap.get(id)!);
    // 实验写法, 不使用 set方法，直接修改状态对象中的值
    //setWideTablePkSelectedMap(wideTablePkSelectedMap);
  };

  /**
   * 加载数据集记录.
   * @param records 数据集记录
   * */
  const loadRecords = (records: DcRecord[]): void => {
    setDcRecords(records);
    loadCompleters(records);
    setLoadV(loadV + 1);
    if (records.length == 0) {
      setDcTableHiddenFlag(true);
    } else {
      setDcTableHiddenFlag(false);
    }
  };

  /**
   * 加载 SQL 补全提示.
   * @param records 数据集记录
   */
  const loadCompleters = (records: DcRecord[]) => {
    const newCompleters: Ace.Completer[] = [];
    for (let cmp of aceInitCompleters) {
      newCompleters.push(cmp);
    }

    if (records) {
      for (let record of records) {
        const newCmp = newCompleter(record);
        newCompleters.push(newCmp);
      }
    }

    aceEditorRef.current!.editor.completers = newCompleters;
  };

  /**
   * 创建 SQL 补全提示.
   * @param record 数据集
   */
  const newCompleter = (record: DcRecord): Ace.Completer => {
    const customCompleter = {
      getCompletions: function (
        _editor: any,
        _session: any,
        _pos: any,
        _prefix: any,
        callback: (arg0: null, arg1: { value: string; meta: string }[]) => void,
      ) {
        // Your custom autocompletion logic here
        var completions = [
          { value: record.name, meta: 'ACDC TABLE' },
          // Add more completions as needed
        ];
        callback(null, completions);
      },
    };

    return customCompleter;
  };

  /**
   * 是否存在数据集.
   *
   * @returns bool
   * */
  const isExistDc = (): boolean => {
    return dcRecords && dcRecords.length != 0;
  };

  //    const customCompleter = {
  //    getCompletions: function(editor, session, pos, prefix, callback) {
  //      // Your custom autocompletion logic here
  //      var completions = [
  //        { value: 'SELECT', meta: 'Keyword' },
  //        { value: 'FROM', meta: 'Keyword' },
  //        { value: 'WHERE', meta: 'Keyword' },
  //        { value: 'table1', meta: 'Table' },
  //        { value: 'table2', meta: 'Table' },
  //        // Add more completions as needed
  //      ];
  //
  //      // If the current token is a valid SQL identifier, add table names to completions
  //      var token = session.getTokenAt(pos.row, pos.column - prefix.length);
  //      if (token && token.type === 'identifier') {
  //        completions = completions.concat([
  //          { value: 'user', meta: 'Table' },
  //          { value: 'order', meta: 'Table' },
  //          // Add more table names as needed
  //        ]);
  //      }
  //
  //      callback(null, completions);
  //    },
  //  };
  //

  /**
   * ACE 组件加载完成，记录默认补全.
   */
  const aceOnLoad = (editor: Ace.Editor) => {
    setAceInitCompleters(editor.completers!);
  };

  /**
   * ACE 组件 onChange 事件.
   *
   * @value 当前编辑器中的内容
   *
   * @ _event 事件对象，对象中包含起始位置和结束位置
   */
  const aceOnChange = (value: string, _event: any): void => {
    // 数据集选中联动效果
    //const matchDcArray = value.match(/(?<=FROM\s+|JOIN\s+)(\w+)/gi);
    matchDc(value);
    // 在没有选择数据集的情况，当警告次数到达限制，进行 alert提示用户,防止频繁提示
    if (!isExistDc()) {
      setAceValueEditWarnCount(aceValueEditWarnCount + 1);
    }
    if (aceValueEditWarnCount > ACE_VALUE_EDIT_WARN_COUNT_LIMIT) {
      message.warn('请选择数据集');
      setAceValueEditWarnCount(0);
    }
    setAceValue(value);
  };

  const matchDc = (value: string): void => {
    const matchDcArray = value.match(/\w+/gi);
    const newSqlUsedRowKeys: string[] = [];
    if (!matchDcArray || matchDcArray.length == 0) {
      if (sqlUsedRowKeys.length > 0) {
        // 减少状态刷新,防止死循环
        setSqlUsedRowKeys([]);
      }
    } else {
      if (dcRecords.length > 0) {
        for (let record of dcRecords) {
          for (let dc of matchDcArray) {
            if (dc.indexOf(record.name) != -1) {
              newSqlUsedRowKeys.push(record.key);
            }
          }
        }
        setSqlUsedRowKeys(newSqlUsedRowKeys);
      }
    }
  };

  const aceOnFocus = () => {
    // 实现首次输入 SQL, 自动清空默认文案的效果
    if (!aceValueEditing) {
      aceEditorRef.current?.editor.setValue(EMPTY_STRING);
      setAceValue(EMPTY_STRING);
      setAceValueEditing(true);
    }
    matchDc(aceValue);
  };

  const aceOnBlur = (): void => {
    matchDc(aceValue);
  };

  /**
   * 重新创建搜索记录,对面包屑路径的末尾展示调整，增加表id,扩展字段赋值.
   *
   * @param record 搜索记录
   * */
  const newDcRecord = (record: SearchRecord): DcRecord => {
    const newRoutes: Route[] = [];
    const dcResourceType: string = record.resourceNode!.resourceType!;
    const dcNameWithId = getDcNameWithId(record);
    const dcNameWithResourceTypeAndId = dcResourceType + B_BNAME_SEPARATOR + dcNameWithId;
    for (let route of record.routes) {
      newRoutes.push(route);
    }
    newRoutes.pop();
    newRoutes.push({ breadcrumbName: dcNameWithResourceTypeAndId, path: '' });

    const projects: Project[] = [];
    for (let projectNoe of record.projectNoes) {
      projects.push({
        id: projectNoe.id,
        name: projectNoe.name,
        owner: projectNoe.owner,
      });
    }

    return {
      key: record.key,
      id: record.resourceNode!.id,
      name: dcNameWithId,
      routes: newRoutes,
      projects: projects,
      chosenPrjTitle: '',
    };
  };

  /**
   * 获取带有Id的数据集名称.
   *
   * @param record 搜索记录
   *
   * @returns 带有ID的数据集名称
   * */
  const getDcNameWithId = (record: SearchRecord): string =>
    record.resourceNode!.name + '_' + record.resourceNode?.id;

  /**
   * 数据集选项删除.
   *
   * @param delKeys 需要删除的 key 集合
   */
  const delDC = (delKeys: (string | number)[]): void => {
    if (!delKeys || delKeys.length == 0 || !isExistDc()) {
      message.info('请选择要删除的数据集');
      return;
    }

    const newRecords: DcRecord[] = [];
    const delKeyMap: Map<string | number, string | number> = new Map();
    for (let key of delKeys) {
      if (sqlUsedRowKeys.findIndex((k) => k === key) > -1) {
        message.info('SQL 语句中已经使用此数据集，请先删除 SQL 语句中引用');
        setSelectedRowKeys([]);
        return;
      }
      delKeyMap.set(key, key);
    }

    for (let record of dcRecords) {
      if (!delKeyMap.has(record.key)) {
        newRecords.push(record);
      }
    }

    loadRecords(newRecords);
  };

  /**
   * 获取数据集 ID 和项目 ID 的映射
   **/
  const getDcIdPrjIdMapping = (): { [key: number]: number } => {
    const dcIdPrjIdMapping = {};
    for (let record of dcRecords) {
      dcIdPrjIdMapping[record.id] = record.chosenPrj!.id;
    }
    return dcIdPrjIdMapping;
  };

  /**
   * DcSearcher 组件 select 事件监听.
   *
   * @param records 选中的记录集合
   **/
  const dcSearcherOnSelect = (records: SearchRecord[]): void => {
    const newRecords: DcRecord[] = [];
    const oldRecordMap: Map<string, string> = new Map();
    for (let record of dcRecords) {
      oldRecordMap.set(record.name, record.name);
      newRecords.push(record);
    }

    if (records) {
      for (let record of records) {
        const name = getDcNameWithId(record);
        if (oldRecordMap.has(name)) {
          message.warn('数据集已存在');
          return;
        }

        newRecords.push(newDcRecord(record));
      }
    }
    loadRecords(newRecords);
  };

  /**
   * 数据集记录双击事件监听.
   *
   * @param record 搜索记录对象
   * */
  const onDcItemDoubleClick = (record: DcRecord): void => {
    const dcName =
      record.routes[record.routes.length - 1].breadcrumbName.split(B_BNAME_SEPARATOR)[1];

    const newAceValue = aceValue.trim() + BLANK + dcName;

    setAceValue(newAceValue);
  };

  /**
   * 宽表申请，步骤1.
   */
  const onStep1 = async () => {
    // 校验是否选择了数据集
    if (dcRecords.length == 0) {
      message.warn('请添加数据集');
      return false;
    }

    // 校验是否填写了 SQL
    if (!aceValueEditing || aceValue.trim() == '') {
      message.warn('请填写SQL');
      return false;
    }

    // 校验 SQL 中不存在 ACDC 的情况
    if (sqlUsedRowKeys.length == 0) {
      message.warn('检测到您的 SQL 语句中不存在 ACDC 的数据集，请检查SQL!');
      return;
    }

    // 未被SQL使用到的数据集处理
    const sqlNotUsedRowKeys: string[] = [];
    for (let record of dcRecords) {
      if (!dcIsUsedBySql(record)) {
        sqlNotUsedRowKeys.push(record.key);
      }
    }
    if (sqlNotUsedRowKeys.length > 0) {
      confirm({
        title: '确定删除吗?',
        icon: <DeleteOutlined />,
        content: '检测到未被 SQL 用到的数据集，是否需要删除？',
        async onOk() {
          delDC(sqlNotUsedRowKeys);
        },
        onCancel() {
          return false;
        },
      });

      return false;
    } else {
      // 表单校验
      await dcTableForm.validateFields();

      // 执行步骤1
      return await doStep1();
    }
  };

  const doStep1 = async (): Promise<boolean> => {
    const dataCollectionIdProjectIdMappings = getDcIdPrjIdMapping();
    const sql = aceValue;

    const reqBody: API.WideTableDetail = {
      dataCollectionIdProjectIdMappings,
      selectStatement: sql,
    };

    const response = await fetch(
      new Request('/api/v1/wide-table?beforeCreation=true', {
        method: 'post',
        headers: {
          'Content-Type': 'application/json;charset=utf-8',
        },
        body: JSON.stringify(reqBody),
      }),
    );

    // promise
    if (response.status == 200) {
      const jsonBody = await response.json();
      const result: API.WideTableDetail = jsonBody;
      const sq = result.subQuery;

      // 添加宽表主键
      if (sq?.columns) {
        const colMap: Map<number, API.WideTableSubqueryColumn> = new Map();
        for (let col of sq.columns) {
          colMap.set(col.id!, col);
        }
        setWideTablePkMap(colMap);
      }
      // 记录子查询对象
      setSubQuery(sq);

      // 下一步处理
      setCurStep(curStep + 1);

      return true;
    } else {
      // 错误处理
      const jsonBody = await response.json();
      setSqlErrorMsg(jsonBody.errorMessage);
      setSqlErrorMsgDwOpenFlag(true);
    }

    return false;
  };

  /**
   * 宽表申请，步骤2.
   */
  const onStep2 = async () => {
    setCurStep(curStep + 1);
    return true;
  };

  /**
   * 宽表申请，步骤3.
   */
  const onStep3 = async () => {
    if (!chosenUserPrj) {
      message.warn('请选择项目');
      return;
    }
    if (chosenWideTablePkMap.size == 0) {
      message.warn('请指定宽表主键');
      return;
    }
    // 表单校验
    await step3Form.validateFields();

    confirm({
      title: '确定提交吗?',
      icon: <DeleteOutlined />,
      content: '创建宽表',
      async onOk() {
        try {
          await doStep3();
          history.push('/wide-table/mgt');
        } catch (error) {}
      },
      onCancel() {},
    });
  };

  const doStep3 = async () => {
    // 宽表名称
    const wideTableName = step3Form.getFieldValue('wideTableName');

    // 申请理由
    const wideTableDescription = step3Form.getFieldValue('wideTableDescription');

    // 宽表主键
    const wideTableCols: API.WideTableColumn[] = [];
    chosenWideTablePkMap.forEach((value) => {
      wideTableCols.push({
        name: SqColBeautifier.getBeautifyName(value),
        type: SqColBeautifier.getBeautifyType(value),
        isPrimaryKey: true,
      });
    });

    // 处理其他列
    wideTablePkMap.forEach((value) => {
      if (wideTableCols.findIndex((item) => item.id === value.id) == -1) {
        wideTableCols.push({
          name: SqColBeautifier.getBeautifyName(value),
          type: SqColBeautifier.getBeautifyType(value),
          isPrimaryKey: false,
        });
      }
    });

    // 所属项目ID
    const projectId = chosenUserPrj!.id;

    // 数据集与项目的 id 映射关系
    const dataCollectionIdProjectIdMappings = getDcIdPrjIdMapping();

    // 宽表 sql
    const wideTableSql = aceValue;

    const reqBody: API.WideTableDetail = {
      name: wideTableName,
      selectStatement: wideTableSql,
      description: wideTableDescription,
      wideTableColumns: wideTableCols,
      dataCollectionIdProjectIdMappings,
      projectId,
    };

    await createWideTable(reqBody);
  };

  /**
   * 数据集是否被 SQL 使用到.
   * @param record 数据集记录
   */
  const dcIsUsedBySql = (record: DcRecord) =>
    sqlUsedRowKeys.findIndex((item) => item === record.key) > -1;

  // 表格列
  const columns = [
    {
      title: '数据集',
      width: '85%',
      dataIndex: 'dc',
      render: (_text: string, record: DcRecord) => [
        <Space key={record.key}>
          <Breadcrumb routes={record.routes} />
        </Space>,
      ],
    },
    {
      title: '数据源负责人',
      width: '10%',
      dataIndex: 'choiceProjectTitle',
      editable: true,
    },
  ];

  const mergedColumns = columns.map((col) => {
    if (!col.editable) {
      return col;
    }
    return {
      ...col,
      onCell: (record: DcRecord) => ({
        record,
        dataIndex: col.dataIndex,
        title: col.title,
      }),
    };
  });

  /**
   * 可编辑单元格组件
   * */
  const EditableCell: React.FC<EditableCellProps> = ({
    dataIndex,
    title,
    record,
    index,
    children,
    ...restProps
  }) => {
    return (
      <td {...restProps}>
        {record && dataIndex == 'choiceProjectTitle' ? (
          <Form.Item
            key={'cell_' + record.id}
            name={'cell_' + record.id}
            style={{ margin: 0 }}
            rules={[
              {
                required: true,
                message: `${title} 必填!`,
              },
            ]}
          >
            <Select
              key={'select_' + record.id}
              showSearch
              allowClear
              showArrow
              placeholder="请选择项目"
              style={{ minWidth: 10 }}
              optionFilterProp="children"
              onSelect={(_value, option) => {
                const chosenPrjTitle = String(option.value);
                const chosenPrjId = Number(option.key);
                const index = record.projects.findIndex((item) => item.id === chosenPrjId);
                record.chosenPrjTitle = chosenPrjTitle;
                record.chosenPrj = record.projects[index];
                setLoadV(loadV + 1);
              }}
              virtual={true}
              // 处理搜索过滤，必须增加
              filterOption={(input, option): boolean => {
                const optionValue: string = option?.value as string;
                return (optionValue ?? '').toLowerCase().includes(input);
              }}
            >
              {record.projects.map((project) => (
                <Option value={project.owner} key={project.id}>
                  <span>{project.owner}</span>
                </Option>
              ))}
            </Select>
          </Form.Item>
        ) : (
          children
        )}
      </td>
    );
  };

  const init = async (): Promise<void> => {
    // 初始化默认展示SQL
    setAceValue(DISPLAY_SQL);

    // 初始化当前用户所属项目数据
    const projects: API.Project[] = await queryProjectByCurrentUser();
    const projectMap: Map<number, API.Project> = new Map();
    for (let project of projects) {
      projectMap.set(project.id!, project);
    }
    setUserProjectMap(projectMap);
  };

  /**
   * 初始化
   */
  useEffect(() => {
    init();
  }, []);

  return (
    <div>
      <StepsForm
        current={curStep}
        stepsProps={{ direction: 'horizontal' }}
        containerStyle={{ width: '100%', height: '100%' }}
        onFinish={async () => {
          return onStep3();
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
                    setCurStep(0);
                  }}
                >
                  上一步
                </Button>,
                <Button type="primary" key="next2" onClick={() => props.onSubmit?.()}>
                  下一步 {'>'}
                </Button>,
              ];
            }
            return [
              <Button
                key="back2"
                onClick={() => {
                  props.onPre?.();
                  setCurStep(1);
                }}
              >
                {'<'} 上一步
              </Button>,
              <Button type="primary" key="save" onClick={() => props.onSubmit?.()}>
                提交申请 √
              </Button>,
            ];
          },
        }}
      >
        <StepsForm.StepForm
          title=" 选择数据源并编辑关联 SQL"
          name="step1"
          style={{ height: '75vh', marginBottom: '20px' }}
          onFinish={async () => {
            return onStep1();
          }}
        >
          <ProCard bordered style={{ height: '40%' }} hoverable>
            <div hidden={dcTableHiddenFlag}>
              <Form form={dcTableForm} component={false}>
                <Table<DcRecord>
                  showHeader={true}
                  key={'dc_table' + loadV}
                  components={{
                    body: {
                      cell: EditableCell,
                    },
                  }}
                  onRow={(record, _index) => {
                    return {
                      onDoubleClick: () => {
                        onDcItemDoubleClick(record);
                      },
                    };
                  }}
                  rowClassName={(record) => {
                    return dcIsUsedBySql(record) ? styles['split-row-select-active'] : '';
                  }}
                  scroll={S_SCROLL}
                  bordered={false}
                  dataSource={dcRecords}
                  columns={mergedColumns}
                  rowSelection={rowSelection}
                  pagination={false}
                />
              </Form>
            </div>
            <div
              hidden={!dcTableHiddenFlag}
              style={{ height: '100%', width: '100%' }}
              onClick={() => {
                setDcSearcherDwOpenFlag(true);
              }}
            >
              <Empty description={'暂无数据，请添加数据集'} />
              {/** <Image preview={false} src="/logo.png" width={500} height={260} /> */}
            </div>
          </ProCard>

          <ProCard
            hoverable
            bordered
            style={{
              height: '60%',
              marginTop: '10px',
              marginBottom: '30px',
            }}
            layout="default"
            actions={[
              <Button.Group key="refs" style={{ display: 'block' }}>
                <Button
                  key="button"
                  icon={<PlusOutlined />}
                  onClick={() => {
                    setDcSearcherDwOpenFlag(true);
                  }}
                >
                  新增数据集
                </Button>
                <Button
                  key="button"
                  icon={<CloseOutlined />}
                  onClick={() => {
                    delDC(selectedRowKeys);
                  }}
                >
                  删除数据集
                </Button>

                <Button
                  key="button"
                  icon={<ScissorOutlined />}
                  onClick={() => {
                    const newSql = format(aceValue, { language: 'mysql' });
                    setAceValue(newSql);
                  }}
                >
                  SQL 美化
                </Button>
              </Button.Group>,
            ]}
          >
            {/*SQL 编辑器*/}
            <AceEditor
              ref={aceEditorRef}
              width="100%"
              height="100%"
              placeholder="请输入SQL:"
              mode="mysql"
              theme="monokai"
              name="ace_sql_editor"
              onLoad={aceOnLoad}
              onChange={aceOnChange}
              onFocus={aceOnFocus}
              onBlur={aceOnBlur}
              value={aceValue}
              fontSize={13}
              showPrintMargin={true}
              showGutter={true}
              highlightActiveLine={true}
              setOptions={{
                enableBasicAutocompletion: true,
                enableLiveAutocompletion: true,
                enableSnippets: true,
                showLineNumbers: true,
                tabSize: 1,
              }}
            />
          </ProCard>
        </StepsForm.StepForm>
        <StepsForm.StepForm
          title="字段血缘关系展示"
          style={{ height: '75vh', marginBottom: '20px' }}
          name="step2"
          onFinish={async () => {
            return onStep2();
          }}
        >
          <>
            <SqColLineageDiagram subQuery={subQuery} />
          </>
        </StepsForm.StepForm>

        <StepsForm.StepForm
          title="配置宽表信息"
          style={{ height: '75vh', marginBottom: '20px' }}
          name="step3"
        >
          <Form
            form={step3Form}
            name="basic"
            style={{ maxWidth: 600 }}
            initialValues={{ remember: true }}
            autoComplete="off"
          >
            <Form.Item
              name="wideTableName"
              rules={[
                { required: true, message: '宽表名称必填写' },
                { max: 50, message: '宽表名称不能超过50字' },
              ]}
            >
              <div style={{ marginTop: 40 }}>
                <Typography.Title level={5}>宽表名称</Typography.Title>
                {/** addonBefore="widetable_" */}
                <Input
                  defaultValue=""
                  placeholder="请输入宽表名称"
                  style={{ width: 811 }}
                  maxLength={50}
                />
              </div>
            </Form.Item>
            <Form.Item
              name="wideTableProject"
              //rules={[{ required: true, message: '项目必填' }]}
            >
              <div style={{ marginTop: 40 }}>
                <Typography.Title level={5}>所属项目</Typography.Title>
                <Select
                  showSearch
                  showArrow
                  placeholder="请选择项目"
                  style={{ minWidth: 811 }}
                  optionFilterProp="children"
                  onSelect={(_value, option) => {
                    const projectId: number = Number(option.key);
                    setChosenUserPrj(userProjectMap.get(projectId));
                  }}
                  virtual={true}
                  // 处理搜索过滤，必须增加
                  filterOption={(input, option): boolean => {
                    const optionValue: string = option?.value as string;
                    return (optionValue ?? '').toLowerCase().includes(input);
                  }}
                >
                  {AcdcCollections.convertMapToList(userProjectMap).map((project) => (
                    <Option value={project.name!} key={project.id}>
                      <span>{project.name!}</span>
                    </Option>
                  ))}
                </Select>
              </div>
            </Form.Item>
            <Form.Item
              name="wideTablePks"
              //rules={[{ required: true, message: '宽表主键必须指定' }]}
            >
              <div style={{ marginTop: 40 }}>
                <Typography.Title level={5}>宽表主键</Typography.Title>
                <Select
                  style={{ width: 811 }}
                  mode="multiple"
                  showSearch
                  allowClear
                  placeholder="请选择宽表主键"
                  optionFilterProp="children"
                  onDeselect={(_value, option) => {
                    const colId: number = Number(option.key);
                    wideTablePkOnDeselect(colId);
                  }}
                  onSelect={(_value, option) => {
                    const colId: number = Number(option.key);
                    wideTablePkOnSelect(colId);
                  }}
                  virtual={true}
                  // 处理搜索过滤，必须增加
                  filterOption={(input, option): boolean => {
                    const optionValue: string = option?.value as string;
                    return (optionValue ?? '').toLowerCase().includes(input);
                  }}
                >
                  {AcdcCollections.convertMapToList(wideTablePkMap).map((col) => (
                    <Option value={SqColBeautifier.getBeautifyName(col)} key={col.id}>
                      <span>{SqColBeautifier.getBeautifyName(col)}</span>
                    </Option>
                  ))}
                </Select>
              </div>
            </Form.Item>
            <Form.Item
              name="wideTableDescription"
              rules={[{ required: true, message: '请输入申请理由' }]}
            >
              <div style={{ marginTop: 40 }}>
                <Typography.Title level={5}>申请理由</Typography.Title>
                <Input.TextArea
                  showCount
                  maxLength={100}
                  onChange={() => {}}
                  placeholder="申请理由"
                  style={{ height: 120, width: 811, resize: 'none' }}
                />
              </div>
            </Form.Item>
          </Form>
        </StepsForm.StepForm>
      </StepsForm>

      {/*数据集搜索*/}
      <Drawer
        width={'100%'}
        open={dcSearcherDwOpenFlag}
        onClose={() => {
          setDcSearcherDwOpenFlag(false);
        }}
        closable={true}
      >
        <DcSearcher
          reqPageSize={20}
          searchScope={SearchScope.ALL}
          multipleChoice={false}
          validateProjectOwner={true}
          includedDataSystemTypes={[DataSystemTypeConstant.TIDB, DataSystemTypeConstant.MYSQL]}
          rootResourceTypes={[
            DataSystemResourceTypeConstant.MYSQL_CLUSTER,
            DataSystemResourceTypeConstant.TIDB_CLUSTER,
          ]}
          onSelect={(records) => {
            dcSearcherOnSelect(records);
            setDcSearcherDwOpenFlag(false);
          }}
        />
      </Drawer>

      {/*错误信息展示*/}
      <Drawer
        title="SQL 执行异常"
        width={'100%'}
        open={sqlErrorMsgDwOpenFlag}
        onClose={() => {
          setSqlErrorMsgDwOpenFlag(false);
        }}
        closable={true}
      >
        {<p style={{ width: '100%', height: '100%', fontSize: 15 }}>{sqlErrorMsg}</p>}
      </Drawer>
    </div>
  );
};

export default WideTableRequisition;
