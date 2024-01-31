import React, { useEffect, useRef, useState } from 'react';
import { Breadcrumb, Input, InputRef, message, Space, Tag } from 'antd';
import ProTable, { ProColumns } from '@ant-design/pro-table';
import ProCard from '@ant-design/pro-card';
import {
  pagedQueryDataSystemResource,
  pagedQueryProject,
  queryDataSystemResourceDefinition,
} from '@/services/a-cdc/api';
import { Route } from 'antd/lib/breadcrumb/Breadcrumb';
import { DataSystemTypeConstant } from '@/services/a-cdc/constant/DataSystemTypeConstant';
import { DataSystemResourceTypeConstant } from '@/services/a-cdc/constant/DataSystemResourceTypeConstant';
const { Search } = Input;

// 滚动条动态计算
const S_SCROLL = {
  y: 'calc(100vh - 300px)',
  scrollToFirstRowOnChange: true,
};

// 搜索翻页加载页数显示
const S_PAGINATION_LIMIT: number = 20;

// 搜索行记录 row key 前缀
const R_ROW_KEY_PREFIX: string = 'r_';

// 搜索路径间隔
const S_PATH_SPACING: string = '\u0020';

// 搜索路径分割符
const S_PATH_SEPARATOR: string = '/';

// 搜索路径多个标题占位符
const S_PATH_MULTI_TITLE_PLACEHOLDER: string = '...';

// Search MODE PROJECT 只搜项目
const S_M_P: number = 1;

// Search MODE RESOURCE 只搜资源
const S_M_R: number = 2;

// Search MODE ROOT RESOURCE 只搜根资源
const S_M_R_R: number = 3;

// Search MODE GLOBAL 全局搜索[项目，资源]
const S_M_G: number = 4;

// Search MODE SILENCE 静默，不执行任何搜索
const S_M_S: number = 5;

// Search  EVENT GO BACK 搜索框事件，搜索上一级
const S_E_GO_BACK: number = 1;

// Search  EVENT KEYWORD 搜索框事件，关键字查询
const S_E_KW: number = 2;

const S_ON_CHANGE: boolean = false;

// 搜索行记录，面包屑 row key 前缀
const B_ROW_KEY_PREFIX: string = 'b_';

// tag 标签 row key 前缀
const T_ROW_KEY_PREFIX: string = 't_';

// 面包屑默认路径
const B_DEFAULT_PATH: string = '#';

const B_DEFAULT_PATH_SEPARATOR: string = '_';

// 面包屑，存在多个 title 展示占位符
const B_MULTI_TITLE_PLACEHOLDER: string = '...';

// 面包屑，root route 面包屑名称前缀
const B_ROOT_ROUTE_BNAME_PREFIX: string = 'PROJECT';

// 面包屑,route 名称分割符
export const B_BNAME_SEPARATOR: string = '\t';

// 资源类型映射
const RESOURCE_TYPE_MAPPING: Map<string, string[]> = new Map([
  [
    DataSystemTypeConstant.MYSQL,
    [
      DataSystemResourceTypeConstant.MYSQL_CLUSTER,
      DataSystemResourceTypeConstant.MYSQL_INSTANCE,
      DataSystemResourceTypeConstant.MYSQL_DATABASE,
      DataSystemResourceTypeConstant.MYSQL_TABLE,
    ],
  ],
  [
    DataSystemTypeConstant.TIDB,
    [
      DataSystemResourceTypeConstant.TIDB_CLUSTER,
      DataSystemResourceTypeConstant.TIDB_SERVER,
      DataSystemResourceTypeConstant.TIDB_DATABASE,
      DataSystemResourceTypeConstant.TIDB_TABLE,
    ],
  ],
  [
    DataSystemTypeConstant.HIVE,
    [
      DataSystemResourceTypeConstant.HIVE,
      DataSystemResourceTypeConstant.HIVE_DATABASE,
      DataSystemResourceTypeConstant.HIVE_TABLE,
    ],
  ],
  [
    DataSystemTypeConstant.KAFKA,
    [DataSystemResourceTypeConstant.KAFKA_CLUSTER, DataSystemResourceTypeConstant.KAFKA_TOPIC],
  ],
  [
    DataSystemTypeConstant.ELASTICSEARCH,
    [
      DataSystemResourceTypeConstant.ELASTICSEARCH_CLUSTER,
      DataSystemResourceTypeConstant.ELASTICSEARCH_INDEX,
    ],
  ],
  [
    DataSystemTypeConstant.STARROCKS,
    [
      DataSystemResourceTypeConstant.STARROCKS_CLUSTER,
      DataSystemResourceTypeConstant.STARROCKS_DATABASE,
      DataSystemResourceTypeConstant.STARROCKS_TABLE,
      DataSystemResourceTypeConstant.STARROCKS_FRONTEND,
    ],
  ],
]);

/**
 * 资源对象的定义.
 */
type ResourceDefinition = {
  resourceType?: string;
  canBeSearchResourceTypes?: string[];
  hasChild?: boolean;
  isLeaf?: boolean;
};

/**
 *
 * 资源对象查询参数.
 */
type ResourceQuery = {
  parentId?: number;
  keyword?: string;
  projectIds?: number[];
  resourceTypes?: string[];
};

/**
 *资源对象节点.

  MYSQL_CLUSTER, MYSQL_INSTANCE, MYSQL_DATABASE, MYSQL_TABLE,

  TIDB_CLUSTER, TIDB_SERVER, TIDB_DATABASE, TIDB_TABLE,

  KAFKA_CLUSTER, KAFKA_TOPIC,

  HIVE, HIVE_DATABASE, HIVE_TABLE,

  SQLSERVER_CLUSTER, SQLSERVER_INSTANCE, SQLSERVER_DATABASE, SQLSERVER_TABLE,

  ORACLE_CLUSTER, ORACLE_INSTANCE, ORACLE_DATABASE, ORACLE_TABLE
*/
export type ResourceNode = {
  // 资源ID
  id: number;
  // 资源名称
  name: string;
  // 资源类型
  resourceType: string;
  // 数据系统类型
  dataSystemType: string;
  // 父节点
  parent?: ResourceNode;
};

/**
 * 项目节点.
 */
export type ProjectNode = {
  // 项目ID
  id: number;
  // 项目名称
  name: string;
  // 项目所属owner
  owner?: string;
};

/**
 * 搜索节点，搜索框中的路径(不包含keyword)都会对应一个搜索节点
 *
 * eg:
 *
 * 1. "project prj1"
 *
 * {
 *   projectNodes:[{id:1,name:'prj1',}],
 *   resourceNode:null
 * }
 *
 *
 * 2. "/project prj1 / db1"
 *
 * {
 *   projectNodes:[{id:1,name:'prj1',}],
 *   resourceNode:{id:3,name:'db1'}
 * }
 *
 */
type SearchNode = {
  /*
   * 项目节点集合，可能有多个项目，可以代码中目前只支持一个项目
   * 项目为root路径,不能为空
   */
  projectNodes: ProjectNode[];

  // 资源对象节点
  resourceNode?: ResourceNode;
};

/**
 * 搜索命令，对搜索框输入的搜索路径，经过路径解析产生的搜索命令
 * 是对 SearchNode 和 keyword 的封装
 */
class SearchCommand {
  // 搜索节点
  searchNode?: SearchNode;

  // 搜索模式,默认只搜索项目
  searchMode?: number = S_M_P;

  // 搜索关键字
  keyword?: string;

  // 是否搜索下一页,默认为false
  searchNextPage?: boolean = false;

  iEvent?: number = S_E_KW;

  isNilString(str?: string): boolean {
    return !str || str.length <= 0 || str.replace(/\s+/g, '').length <= 0;
  }

  setKwToEmptyIfNeed(keyword?: string): string {
    return !keyword ? '' : keyword;
  }

  setKwToNoneIfNeed(keyword?: string): string | undefined {
    return this.isNilString(keyword) ? undefined : keyword;
  }

  constructor(
    searchNode?: SearchNode,
    searchMode?: number,
    keyword?: string,
    searchNextPage?: boolean,
    iEvent?: number,
  ) {
    if (searchNode) this.searchNode = searchNode;
    if (searchMode) this.searchMode = searchMode;
    if (keyword) this.keyword = keyword;
    if (searchNextPage !== undefined) this.searchNextPage = searchNextPage;
    if (iEvent) this.iEvent = this.iEvent;
  }

  /**
   * 需要展示的 keyword
   */
  dKw(): string {
    return this.setKwToEmptyIfNeed(this.keyword);
  }
  /**
   * 需要搜索的 keyword
   */
  sKw(): string | undefined {
    return this.setKwToNoneIfNeed(this.keyword);
  }
}

/**
 * 搜索页码
 */
type SearchPagination = {
  // 记录资源的当前请求分页
  resourcePageNum: number;

  // 记录项目的当前请求分页
  projectPageNum: number;
};

/**
 * 搜索执行前声明
 **/
class SearchTracker {
  command: SearchCommand;
  records: SearchRecord[] = [];
  beginIndex: number;

  hasProject: boolean = false;
  hasResource: boolean = false;

  private bPagination: SearchPagination;
  private cPagination: SearchPagination;
  private aPagination: SearchPagination;

  constructor(
    command: SearchCommand,
    paginationState: SearchPagination,
    recordsState?: SearchRecord[],
  ) {
    this.command = command;

    const { searchNextPage } = this.command;
    if (!searchNextPage) {
      this.bPagination = { projectPageNum: 1, resourcePageNum: 1 };
      this.cPagination = { ...this.bPagination };
      this.aPagination = { ...this.cPagination };
      this.records = [];
      this.beginIndex = 1;
    } else {
      this.cPagination = { ...paginationState };
      this.bPagination = {
        projectPageNum: this.cPagination.projectPageNum + 1,
        resourcePageNum: this.cPagination.resourcePageNum + 1,
      };
      this.aPagination = { ...this.cPagination };

      if (recordsState && recordsState.length != 0) {
        for (let record of recordsState) {
          this.records.push(record);
        }
      }
      this.beginIndex = this.records.length == 0 ? 1 : this.records.length + 1;
    }
  }

  doneProjectSearch(hasProject: boolean): void {
    this.hasProject = hasProject;
    this.aPagination = {
      ...this.cPagination,
      projectPageNum: this.bPagination.projectPageNum,
    };
  }
  doneResourceSearch(hasResource: boolean): void {
    this.hasResource = hasResource;
    this.aPagination = {
      ...this.cPagination,
      resourcePageNum: this.bPagination.resourcePageNum,
    };
  }

  getToSearchPaging(): SearchPagination {
    return this.bPagination;
  }

  getCurrentPaging(): SearchPagination {
    return this.aPagination;
  }
}

/**
 * 搜索记录
 */
export type SearchRecord = {
  // row key
  key: string;
  // 资源对象节点
  resourceNode?: ResourceNode;

  // 项目节点，可能存在多个项目的情况
  projectNoes: ProjectNode[];

  // 用于面包屑展示
  routes: Route[];

  // 权重，用于排序或者其他优先级选择
  weight: number;

  // 标签
  tags?: string[];

  // 是否启用复选框
  enableCheck: boolean;

  // 扩展字段
  extras?: any;
};

/**
 *搜索范围枚举
 */
const SearchScope = {
  CURRENT_USER: 'CURRENT_USER',
  ALL: 'ALL',
};

export { SearchScope };

/**
 *搜索组件属性
 */
export type DcSearcherProps = {
  // 请求数据分页大小
  reqPageSize?: number;

  // 是否支持多选
  multipleChoice?: boolean;

  // 数据系统过滤
  includedDataSystemTypes: string[];

  // root reosurce 资源类型
  rootResourceTypes: string[];

  // 是否校验项目 owner
  validateProjectOwner: boolean;

  // 搜索范围
  searchScope: string;

  // 数据集选中事件监听
  onSelect: (srList: SearchRecord[]) => void;
};

// 数据集搜索组件定义
const DcSearcher: React.FC<DcSearcherProps> = (dcSearcherProps) => {
  const {
    // 查询范围,默认只查询当前用户所属的资源
    searchScope = SearchScope.CURRENT_USER,
    // 请求分页大小，默认为 1
    reqPageSize = 1,
    multipleChoice = false,
    includedDataSystemTypes,
    rootResourceTypes,
    validateProjectOwner,
    onSelect: dcOnSelect,
  } = dcSearcherProps;

  const [searchResourceTypesState, setSearchResourceTypesState] = useState<string[]>([]);

  // 资源定义
  const [resourceDefinitionState, setResourceDefinitionState] = useState<
    Map<string, ResourceDefinition>
  >(new Map());

  // 搜索请求页码
  const [searchPaginationState, setSearchPaginationState] = useState<SearchPagination>({
    projectPageNum: 1,
    resourcePageNum: 1,
  });

  // 搜索数据加载刷新 version 控制
  const [searchReloadVState, setSearchReloadVState] = useState<number>(-99);

  // 滚动条配置
  const [scrollConfigState] = useState<Object>(S_SCROLL);

  // 搜索框的值
  const [searchInputVlaueState, setSearchInputValueState] = useState<string>('');

  // 上一次的搜索节点
  const [lastSearchNodeState, setLastSearchNodeState] = useState<SearchNode>();

  // 上一次搜索命令
  const [lastSearchCommandState, setLastSearchCommandState] = useState<SearchCommand>();

  // 选中的搜索记录
  const [selectedSearchRecordsState, setSelectedSearchRecordsState] = useState<SearchRecord[]>([]);

  // 搜索记录
  const [searchRecordsState, setSearchRecordsState] = useState<SearchRecord[]>([]);

  // 搜索,加载遮罩
  const [searchLoadingState, setSearchLoadingState] = useState<boolean>(false);

  // 搜索,加载下一页数据隐藏控制
  const [searchLoadMoreState, setSearchLoadMoreState] = useState<boolean>(false);

  // 搜索框重新获取焦点使用
  const searchInputRef = useRef<InputRef>();

  /*
   * 配合 userEffect 使用，在页面"第一次渲染"的时候初始化数据
   * 只初"始化一次",因为 initState 初始化之后不会再改变.
   **/
  const [initState] = useState<any>();

  /**
   * 生成标签.
   *
   * @param tags 标签展示数据集
   *
   * @returns  JSX 标签集合
   */
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
   * 查询项目.
   *
   * @param pageNum 当前页码
   *
   * @param keyword 查询关键字
   *
   * @returns 项目列表
   **/
  const searchProject = async (pageNum: number, keyword?: string) => {
    const projectQuery = {
      pageSize: reqPageSize,
      scope: searchScope,
      deleted: false,
      current: pageNum,
      name: keyword,
    };

    const pageProject = await pagedQueryProject(projectQuery);
    const projects = pageProject?.data;
    return projects ? projects : [];
  };

  /**
   * 查询资源.
   *
   * @param pageNum 当前页码
   *
   * @param keyword 查询关键字
   *
   * @returns 项目列表
   **/
  const searchResource = async (pageNum: number, query: ResourceQuery) => {
    const resourceQuery: API.DataSystemResourceQuery = {
      pageSize: reqPageSize,
      scope: searchScope,
      deleted: false,
      current: pageNum,
      name: query.keyword,
      projectIds: query.projectIds,
      resourceTypes: query.resourceTypes,
      parentResourceId: query.parentId,
    };

    const pagedResource = await pagedQueryDataSystemResource(resourceQuery);
    const resources = pagedResource?.data;
    return resources ? resources : [];
  };

  /**
   * 生成项目节点.
   *
   * @param project 项目
   *
   * @returns 项目节点
   **/
  const generateProjectNode = (project: API.Project) => {
    const projectNode: ProjectNode = {
      id: project.id!,
      name: project.name!,
      owner: project.ownerName,
    };

    return projectNode;
  };

  /**
   * 生成资源节点.
   *
   * @param resource 资源
   *
   * @returns 资源节点
   **/
  const generateResourceNode = (resource: API.DataSystemResource) => {
    const resourceNode: ResourceNode = {
      id: resource.id!,
      name: resource.name!,
      resourceType: resource.resourceType!,
      dataSystemType: resource.dataSystemType!,

      // 递归添加父级节点
      parent: resource.parentResource ? generateResourceNode(resource.parentResource) : undefined,
    };
    return resourceNode;
  };

  /**
   * 资源是否可以成为搜索节点.
   *
   * <p>
   * eg: 数据库实例不能成为搜索节点,数据库实例的父节点数据库集群才可以成为搜索节点
   * </p>
   *
   * @param resourceType 资源类型
   *
   * @returns 是否可以成为搜索节点 true|false
   **/
  const resourceCanBeSearchNode = (resourceType: string) => {
    const rd = resourceDefinitionState.get(resourceType)!;
    return rd && !(!rd.hasChild && !rd.isLeaf);
  };

  /**
   * 资源是否是叶子节点.
   *
   * @param resourceType 资源类型
   *
   * return true|false
   */
  const resourceIsLeaf = (resourceType: string) => {
    const rd = resourceDefinitionState.get(resourceType)!;
    return rd && rd.isLeaf;
  };

  /**
   * 计算 search node 权重值.
   *
   * @param projectNodes 项目节点集合
   *
   * @param resourceNode 资源节点
   *
   * @returns 权重值
   */
  const calculateSearchRecordWeight = (
    projectNodes: ProjectNode[],
    resourceNode?: ResourceNode,
  ) => {
    /*TODO
     *
     * 项目名称的排序暂时不需要，只考虑路径的长短即可,存在多个项目情况,
     * 优先级排低
     */
    const projectNodeWeight = projectNodes.length;

    let resourceWeight = 0;
    let node: ResourceNode | undefined = resourceNode;

    // 从当前节点向父级节点遍历
    for (node; node; node = node.parent) {
      resourceWeight++;
    }

    return projectNodeWeight + resourceWeight;
  };

  /**
   * 获取选中搜索记录对应的 rowkey集合.
   *
   * <p>
   *
   * 这个方法主要是给选择框批量选中使用.
   * 解决:checkbox 的选择事件，和行选择事件不能很好的结合.
   *
   * </p>
   *
   * @param selectedSearchRecords 选中的搜索记录
   */
  const getSelectedRowKeys = (selectedSearchRecords: SearchRecord[]) => {
    const keys: string[] = [];
    for (let record of selectedSearchRecords) {
      keys.push(record.key);
    }

    return keys;
  };

  /**
   * 挂载搜索节点.
   *
   * <p>
   * 1. 把搜索路径挂载到搜索节点上，搜索框中的搜索路径对应唯一的一个搜索节点
   *
   * 2.搜索节点中包含后台搜索接口所需要的搜索参数
   *
   * 可以理解为：linux系统，所有的硬件设备都需要挂载到操作系统目录树下，才能访问
   * 搜索路径就相当与文件系统目录树，是我们能够获取到搜索节点的一种"表达方式"
   * </p>
   *
   * @param searchNode 搜索节点
   *
   * @returns 挂载成功的搜索路径
   * */
  const mountSearchNode = (searchNode: SearchNode) => {
    // case: 1.只有项目节点， 2.存在项目节点和资源节点
    let path: string = '';

    const { projectNodes, resourceNode } = searchNode;
    if (resourceNode) {
      let rNode: ResourceNode | undefined = resourceNode;
      for (rNode; rNode; rNode = rNode.parent) {
        path = rNode.name + S_PATH_SPACING + S_PATH_SEPARATOR + S_PATH_SPACING + path;
      }
    }

    // 增加项目根路径
    const pNodeName =
      projectNodes.length > 1 ? S_PATH_MULTI_TITLE_PLACEHOLDER : projectNodes[0].name;
    path = pNodeName + S_PATH_SPACING + S_PATH_SEPARATOR + S_PATH_SPACING + path;

    return path;
  };

  /**
   * 解析搜索框输入.
   *
   * @param input 搜索框输入值
   *
   * @returns 搜索命令对象
   */
  const parseSearchInput = (input: string, lastSearchNode?: SearchNode) => {
    // 没有输入的情况，加载默认数据
    if (!input || input.length <= 0 || input.replace(/\s+/g, '').length <= 0) {
      const command = new SearchCommand();
      command.searchMode = S_M_P;
      return command;
    }

    /*
     * case1: 不存在搜索节点，输入 keyword 执行全局搜索
     *
     * case2: 存在搜索节点,搜索路径改变
     *  a. 搜索路径回退上一级路径
     *  b. 搜索路径非法修改
     *
     * case3: 存在搜索节点,搜索路径未变化，输入 keyword 进行搜索
     */

    // 以下逻辑，搜索框中的一定存在合法的 kw 值，无需再继续判断kw的合法性

    // case1
    if (!lastSearchNode) {
      const command = new SearchCommand();
      command.keyword = input;
      command.searchMode = S_M_G;
      return command;
    }

    // case2 case3
    const cmpInput = mountSearchNode(lastSearchNode);
    let i = 0;
    for (i; i < cmpInput.length && cmpInput[i] == input[i]; i++) {}
    if (i == cmpInput.length - 1) {
      // case2:a
      if (!lastSearchNode.resourceNode) {
        // 回退，清除所有搜索节点
        const command = new SearchCommand();
        command.searchMode = S_M_P;
        command.iEvent = S_E_GO_BACK;
        return command;
      } else if (!lastSearchNode.resourceNode?.parent) {
        // 回退到项目节点
        const command = new SearchCommand();
        command.searchNode = { projectNodes: lastSearchNode.projectNodes };
        command.searchMode = S_M_R_R;
        command.iEvent = S_E_GO_BACK;
        return command;
      } else {
        // 回退上一级搜索节点
        const command = new SearchCommand();
        command.searchNode = {
          resourceNode: lastSearchNode.resourceNode?.parent,
          projectNodes: lastSearchNode.projectNodes,
        };
        command.searchMode = S_M_R;
        command.iEvent = S_E_GO_BACK;
        return command;
      }
    } else if (i < cmpInput.length - 1) {
      // case2:b
      // TODO 如果非法修改了搜索路径，则使用上次搜索节点替换,并且删除用户输入的 keyword
      const command = new SearchCommand();
      command.searchNode = lastSearchNode;
      command.searchMode = S_M_S;
      return command;
    } else if (i == cmpInput.length) {
      // case3:
      let kw = input.substring(cmpInput.length, input.length);
      const mode = lastSearchNode.resourceNode ? S_M_R : S_M_R_R;
      return new SearchCommand(lastSearchNode, mode, kw, false);
    } else {
      throw Error('An error was encountered parsing search input');
    }
  };

  /**
   * 搜索记录选中事件监听.
   *
   * @param searchRecord 搜索记录
   **/
  const searchRecordOnSelect = (record: SearchRecord) => {
    const { resourceNode, projectNoes } = record;

    // 项目节点，或者非叶子资源节点执行 S_M_R_R 或者 S_M_R 搜索
    if ((projectNoes && !resourceNode) || !resourceIsLeaf(resourceNode!.resourceType!)) {
      doSearchRecordOnSelect({ projectNodes: projectNoes, resourceNode: resourceNode });
      return;
    }

    const selectedSearchRecords: SearchRecord[] = [];
    if (multipleChoice) {
      // 多选模式
      let containThisRecord: boolean = false;
      for (let sRecord of selectedSearchRecordsState) {
        if (sRecord.key === record.key) {
          containThisRecord = true;
          continue;
        }
        selectedSearchRecords.push(sRecord);
      }

      if (!containThisRecord) {
        selectedSearchRecords.push(record);
      }
    } else {
      // 单选模式
      selectedSearchRecords.push(record);
    }

    setSelectedSearchRecordsState(selectedSearchRecords);
    setSearchReloadVState(searchReloadVState + 1);

    doCallBack(selectedSearchRecords);
  };

  const doSearchRecordOnSelect = (searchNode: SearchNode) => {
    const cmpInput = mountSearchNode(searchNode);
    setSearchInputValueState(cmpInput);
    setLastSearchNodeState(searchNode);
    const command = parseSearchInput(cmpInput, searchNode);
    doSearch(command);
  };

  /**
   * 搜索组件回调函数，给外部组建提供搜索数据.
   * @param records, 搜索记录集合
   */
  const doCallBack = (records: SearchRecord[] = []) => {
    // TODO 目前不支持批量选择的资源来自多个项目
    //
    const anyRecordPrjMapping: Map<string, String> = new Map();
    const anyPrjNodes = records?.length >= 1 ? records[0].projectNoes : [];
    for (let anyP of anyPrjNodes) {
      anyRecordPrjMapping.set(anyP.name, anyP.name);
    }

    for (let record of records) {
      for (let p of record.projectNoes) {
        // owner 校验
        if (!p.owner && validateProjectOwner) {
          message.warn('项目: ' + p.name + ', 不存在负责人,请联系ACDC团队');
          setSelectedSearchRecordsState([]);
          setSearchReloadVState(searchReloadVState + 1);
          return;
        }

        // 选择不同项目校验
        if (!anyRecordPrjMapping.has(p.name)) {
          message.warn('数据集只能属于同一个项目，请重新选择');
          setSelectedSearchRecordsState([]);
          setSearchReloadVState(searchReloadVState + 1);
          return;
        }
      }
    }

    dcOnSelect(records);
  };

  /**
   * 搜索框输入变化事件监听.
   *
   * @param input 当前输入内容,只要内容变化都会触发一次事件
   * */
  const searchInputOnChange = (input: string) => {
    const command = parseSearchInput(input, lastSearchNodeState);
    doSearchInputOnChange(command, S_ON_CHANGE);
  };
  const doSearchInputOnChange = (command: SearchCommand, execSearch: boolean) => {
    const { searchNode } = command;
    let searchPath = '';
    if (searchNode) {
      searchPath = mountSearchNode(searchNode);
    }

    const input = searchPath + command.dKw();
    setSearchInputValueState(input);

    if (execSearch || command.iEvent == S_E_GO_BACK) doSearch(command);
  };

  /**
   * 分页加载次数限制.
   */
  const isReachedPagingLimit = () => {
    const pageCount = searchPaginationState.resourcePageNum;
    return pageCount > S_PAGINATION_LIMIT;
  };

  /**
   * 加载更多事件监听.
   *
   * <p>
   * 1. 需要获取到上一次搜索命令，然后继续搜索下一页
   *
   * 2. 更改搜索命令标识，继续上次搜索条件请求下一页数据
   * </p>
   *
   **/
  const loadMoreOnClick = async () => {
    if (isReachedPagingLimit()) {
      message.info('请尝试"关键字"搜索加快查找效率!');
      return;
    }

    if (lastSearchCommandState) {
      doSearch(
        new SearchCommand(
          lastSearchCommandState.searchNode,
          lastSearchCommandState.searchMode,
          lastSearchCommandState.keyword,
          true,
        ),
      );
    }
  };

  /**
   * 搜索事件监听.
   *
   * @param 搜索框输入值
   */
  const onSearch = (input: string) => {
    const command = parseSearchInput(input, lastSearchNodeState);
    doSearch(command);
  };

  /**
   * 执行搜索.
   *
   * @param searchCommand 搜索命令
   * @returns 搜索记录
   **/
  const doSearch = async (searchCommand: SearchCommand) => {
    setSearchLoadingState(true);
    await execSearchCommand(searchCommand);
    setSearchLoadingState(false);
    // 注意：这个获取焦点的事件要当到 setSearchLoadingState(false)后面执行,否则会被之前的状态覆盖
    searchInputRef.current?.focus();
  };

  /**
   * 执行搜索命令.
   *
   * @param searchCommand 搜索命令
   * @returns 搜索记录
   **/
  const execSearchCommand = async (searchCommand: SearchCommand) => {
    // 搜索记录 row key 生成
    const rowKey = (index: number) => R_ROW_KEY_PREFIX + index;

    // 项目节点面包屑生成
    const generateProjectRoute = (
      recordIdx: number,
      routeIdx: number,
      nodes: ProjectNode[],
      children?: Route[],
    ) => {
      const bname =
        B_ROOT_ROUTE_BNAME_PREFIX +
        B_BNAME_SEPARATOR +
        (nodes.length > 1 ? B_MULTI_TITLE_PLACEHOLDER : nodes[0].name);

      let route: Route = {
        breadcrumbName: bname,
        path: B_DEFAULT_PATH + recordIdx + B_DEFAULT_PATH_SEPARATOR + routeIdx,
        children: children,
      };
      return route;
    };

    // 资源节点面包屑生成
    const generateResourceRoute = (recordIdx: number, routeIdx: number, node: ResourceNode) => {
      const bname = node.resourceType + B_BNAME_SEPARATOR + node.name;
      let route: Route = {
        breadcrumbName: bname,
        path: B_DEFAULT_PATH + recordIdx + B_DEFAULT_PATH_SEPARATOR + routeIdx,
      };
      return route;
    };

    // 解析项目接口返回数据
    const parseProject = (index: number, project: API.Project) => {
      const projectNode: ProjectNode = generateProjectNode(project);
      const weight = calculateSearchRecordWeight([projectNode]);
      const route: Route = generateProjectRoute(index, 1, [projectNode]);
      const searchRecord: SearchRecord = {
        key: rowKey(index),
        projectNoes: [projectNode],
        weight: weight,
        enableCheck: false,
        routes: [route],
      };

      return searchRecord;
    };

    // 解析资源接口返回数据
    const parseResource = (index: number, resource: API.DataSystemResource) => {
      // 项目节点,如果当前节点没有项目，递归遍历父级节点获取
      const projectNodes: ProjectNode[] = [];
      let rs: API.DataSystemResource = resource;
      for (rs; !rs.projects || rs.projects?.length <= 0; rs = rs.parentResource!) {}

      let projects: API.Project[] = rs.projects;

      for (let project of projects) {
        projectNodes.push(generateProjectNode(project));
      }

      // 资源节点
      const resourceNode: ResourceNode = generateResourceNode(resource);

      // 权重值
      const weight = calculateSearchRecordWeight(projectNodes, resourceNode);

      // 1.判断是否需要打标签,打标签的节点不展示到面包屑的路径中
      // 2.递归遍历生成层级的面包屑路径
      const routes: Route[] = [];
      const tags: string[] = [];
      let rNode: ResourceNode | undefined = resourceNode;
      let routeIndex = 1;
      for (rNode; rNode; rNode = rNode.parent) {
        if (!resourceCanBeSearchNode(rNode.resourceType)) {
          tags.push(rNode.name);
        } else {
          routes.unshift(generateResourceRoute(index, routeIndex, rNode));
          routeIndex++;
        }
      }

      // 项目节点特殊处理,项目节点作为root节点存在
      const rootRouteChildren: Route[] = [];
      if (projectNodes.length > 1) {
        for (let node of projectNodes) {
          rootRouteChildren.push(generateProjectRoute(index, routeIndex, [node]));
          routeIndex++;
        }
      }
      const rootRoute: Route = generateProjectRoute(
        index,
        ++routeIndex,
        projectNodes,
        rootRouteChildren,
      );
      routes.unshift(rootRoute);

      const searchRecord: SearchRecord = {
        key: rowKey(index),
        projectNoes: projectNodes,
        resourceNode: resourceCanBeSearchNode(resourceNode.resourceType)
          ? resourceNode
          : resourceNode.parent,
        weight: weight,
        enableCheck: resourceIsLeaf(resourceNode.resourceType)!,
        routes: routes,
        tags: tags,
      };

      return searchRecord;
    };

    // 批量添加项目搜索记录
    const batchAddProject = (projects: API.Project[], tracker: SearchTracker) => {
      let index = tracker.beginIndex;
      let records = tracker.records;

      for (let project of projects) {
        const searchRecord = parseProject(index, project);
        records.push(searchRecord);
        index++;
      }

      tracker.beginIndex = index;
      tracker.records = records;
      tracker.doneProjectSearch(reqPageSize == projects.length);
    };

    // 批量添加资源搜索记录
    const batchAddResource = (resources: API.DataSystemResource[], tracker: SearchTracker) => {
      let index = tracker.beginIndex;
      let records = tracker.records;
      for (let resource of resources) {
        const searchRecord = parseResource(index, resource);
        records.push(searchRecord);
        index++;
      }
      tracker.beginIndex = index;
      tracker.records = records;
      tracker.doneResourceSearch(reqPageSize == resources.length);
    };

    // 加载搜索记录
    const loadRecords = (records: SearchRecord[]) => {
      setSearchRecordsState(records);
      setSearchReloadVState(searchReloadVState + 1);
    };

    // 搜索记录排序
    const sort = (records: SearchRecord[]) => {
      records.sort((n1, n2) => {
        return n1.weight - n2.weight;
      });
    };

    // 启动搜索跟踪器，设置一些搜索前必要的参数准备,记录搜索过程产生的必要参数
    const beforeSearch = (searchCommand: SearchCommand) => {
      return new SearchTracker(searchCommand, searchPaginationState, searchRecordsState);
    };

    // 搜索完成后,必要数据设置
    const afterSearch = (tracker: SearchTracker) => {
      const { records, hasProject, hasResource, command } = tracker;

      sort(records);

      setSearchPaginationState(tracker.getCurrentPaging());
      setSearchLoadMoreState(!hasProject && !hasResource);
      setLastSearchNodeState(command?.searchNode);
      setLastSearchCommandState(command);
      setSelectedSearchRecordsState([]);
      loadRecords(records);
    };

    // 搜索模式必要参数校验
    const assertSMP = (_: SearchCommand) => {};
    const assertSMG = (_: SearchCommand) => {};
    const assertSMR = (searchCommand: SearchCommand) => {
      const { searchNode } = searchCommand;
      if (
        !searchNode ||
        !searchNode.projectNodes ||
        (searchNode.resourceNode &&
          (!searchNode.resourceNode.id ||
            !searchNode.resourceNode.resourceType ||
            !searchNode.resourceNode.dataSystemType))
      )
        throw new Error(
          'require ProjectNode[id],or (ProjectNode[id],Resource[id,resourceType,dataSystemType]',
        );
    };
    const assertSMRR = (searchCommand: SearchCommand) => {
      const { searchNode } = searchCommand;
      if (!searchNode || !searchNode.projectNodes) throw new Error('require ProjectNode[id]');
    };

    // 获取项目id 集合

    const obtainProjectIds = (projectNodes?: ProjectNode[]) => {
      const ids: number[] = [];

      if (!projectNodes) {
        return ids;
      }

      for (let node of projectNodes) {
        ids.push(node.id);
      }
      return ids;
    };

    /*
     * 主流程逻辑处理:
     *
     * 搜索模式:
     * 1. 搜索项目[]
     * 2. 搜索项目下的根资源:[ProjectNodes]
     * 3. 搜索资源:ProjectNodes,ResourceNode,[keyWord]
     * 4. 全局搜索:[keyword]
     * 5. 静默,不执行搜索:[]
     */

    // before search
    const tracker = beforeSearch(searchCommand);
    const command = tracker.command;
    const pagination = tracker.getToSearchPaging();

    // do search
    const { searchMode, searchNode } = searchCommand;
    switch (searchMode) {
      case S_M_P: // case1
        assertSMP(searchCommand);
        batchAddProject(await searchProject(pagination.projectPageNum), tracker);
        break;
      case S_M_R_R: // case2
        assertSMRR(searchCommand);
        batchAddResource(
          await searchResource(pagination.resourcePageNum, {
            keyword: command.sKw(),
            // TODO 目前只支持一个项目的搜索
            projectIds: obtainProjectIds(searchNode?.projectNodes),
            resourceTypes: rootResourceTypes,
          }),
          tracker,
        );
        break;
      case S_M_R: // case3
        assertSMR(searchCommand);
        const canBeSearchResourceTypes = resourceDefinitionState.get(
          searchNode?.resourceNode?.resourceType!,
        )?.canBeSearchResourceTypes;
        batchAddResource(
          await searchResource(pagination.resourcePageNum, {
            keyword: command.sKw(),
            parentId: searchNode?.resourceNode?.id,
            // 解决查询集群的情况，会把实例信息查询出来的问题
            resourceTypes: canBeSearchResourceTypes,
            // TODO 目前只支持一个项目的搜索
            projectIds: obtainProjectIds(searchNode?.projectNodes),
          }),
          tracker,
        );
        break;
      case S_M_G: // case4
        assertSMG(searchCommand);
        batchAddProject(await searchProject(pagination.projectPageNum, command.dKw()), tracker);
        batchAddResource(
          await searchResource(pagination.resourcePageNum, {
            keyword: command.sKw(),
            resourceTypes: searchResourceTypesState,
          }),
          tracker,
        );
        break;
      case S_M_S: // case5
        return;
      default:
        throw new Error('Unknown search mode: ' + searchMode);
    }

    // after search
    afterSearch(tracker);
  };

  /**
   * 初始化搜索结果默认值,页面首次进入默认加载项目.
   * */
  const initDefaultSearchRecords = async () => {
    doSearch(new SearchCommand());
  };

  /**
   * 初始化资源定义配置.
   **/
  const initResourceDefinition = async () => {
    // 资源定义: <ResourceType,ResourceDefinition>
    const resourceDefinition: Map<string, ResourceDefinition> = new Map();
    // 递归遍历
    const recursiveTraversal = (
      difinitions: API.DataSystemResourceDefinition[],
      resourceDefinition: Map<string, ResourceDefinition> = new Map(),
    ) => {
      for (let difination of difinitions) {
        // 1.递归边界: 如果存在子节点继续递归处理，否则处理当前节点
        const canBeSearchResourceTypes: string[] = [];
        if (difination.children) {
          const children: API.DataSystemResourceDefinition[] = [];
          for (let key in difination.children) {
            let rd = difination.children[key];
            if (rd.dataCollection || rd.hasDataCollectionChild) {
              canBeSearchResourceTypes.push(rd.type);
            }
            children.push(rd);
          }
          recursiveTraversal(children, resourceDefinition);
        }

        // 2.数据转换
        resourceDefinition.set(difination.type, {
          resourceType: difination.type,
          hasChild: difination.hasDataCollectionChild,
          isLeaf: difination.dataCollection,
          canBeSearchResourceTypes: canBeSearchResourceTypes,
        });
      }
    };

    const difinitions =
      (await queryDataSystemResourceDefinition()) as API.DataSystemResourceDefinition[];

    recursiveTraversal(difinitions, resourceDefinition);

    // 更新状态，保存difination 到 state 中
    setResourceDefinitionState(resourceDefinition);
  };

  /**
   * 初始化支持搜索的资源类型.
   **/
  const initSearchResourceTypes = () => {
    const searchResourceTypes: string[] = [];
    for (let type of includedDataSystemTypes) {
      const resourceTypes = RESOURCE_TYPE_MAPPING.get(type);
      if (resourceTypes) {
        searchResourceTypes.push(...resourceTypes);
      }
    }

    setSearchResourceTypesState(searchResourceTypes);
  };

  /**
   * 组件初始化数据,在组建被卸载之前只执行一次初始化操作.
   *
   * 1. 默认数据加载
   * 2. 加载资源声明
   * 3. 加载搜索资源类型
   * */
  useEffect(() => {
    initDefaultSearchRecords();
    initResourceDefinition();
    initSearchResourceTypes();
  }, [initState]);

  /**
   *搜索结果,面包屑展示.
   */
  const columns: ProColumns<SearchRecord>[] = [
    {
      render: (_text, record, _, _action) => [
        <Space key={B_ROW_KEY_PREFIX + record.key}>
          <Breadcrumb routes={record.routes} />
          {generateTags(record)}
        </Space>,
      ],
    },
  ];

  /**
   * 选择框 radio 和checkBox 控制.
   *
   * <p>
   * 1. 控制是否可以点击，只有资源类型为叶子节点才可以点击
   *
   * 2. onChnage 事件会获取到当前选中的所有记录，所以历史状态中记录的选中记录，需要
   * 按照 onChange事件中的强制同步一次，保证选择的记录数据一致性
   *
   * </p>
   *
   **/
  const rowSelection = {
    // 注释该行则默认不显示下拉选项
    // selections: [Table.SELECTION_ALL, Table.SELECTION_INVERT],
    selectedRowKeys: getSelectedRowKeys(selectedSearchRecordsState),
    getCheckboxProps: (record: SearchRecord) => ({
      disabled: !record.enableCheck,
    }),

    onChange: (_keys: React.Key[], records: SearchRecord[]) => {
      // 1. records 为当前处于选中状态的所有记录
      // 2. 需要把历史保存的选中记录进行同步更新
      const currentSelectedRecords: SearchRecord[] = records ? records : [];
      const newSelectedRecords: SearchRecord[] = [];
      for (let rc of currentSelectedRecords) {
        newSelectedRecords.push(rc);
      }
      setSelectedSearchRecordsState(newSelectedRecords);
      setSearchReloadVState(searchReloadVState + 1);

      doCallBack(newSelectedRecords);
    },
  };

  return (
    <div>
      <Search
        size="large"
        placeholder="请输入查询条件:   项目 / 实例地址 / 集群 / 数据库 / 数据集"
        onSearch={onSearch}
        value={searchInputVlaueState}
        loading={searchLoadingState}
        enterButton
        disabled={searchLoadingState}
        ref={searchInputRef}
        onChange={(event) => {
          searchInputOnChange(event.target.value);
        }}
      />
      <ProCard bordered>
        <ProTable<SearchRecord>
          columns={columns}
          rowSelection={{
            type: multipleChoice ? 'checkbox' : 'radio',
            ...rowSelection,
          }}
          tableAlertRender={({}) => {
            return (
              <>
                {/*<span style={{ color: 'cyan' }}>注意:&nbsp;&nbsp;&nbsp;</span>*/}
                <span style={{ color: 'gray' }}>重新搜索后，之前选中的数据集会被清空</span>
              </>
            );
          }}
          //tableAlertOptionRender={() => {
          //  return (
          //  <></>
          //  );
          //}}
          params={{
            refreshVersion: searchReloadVState,
          }}
          request={async () => {
            return {
              success: true,
              data: searchRecordsState,
            };
          }}
          onLoad={(_records) => {}}
          // 根据屏幕动态计算高度，出现滚动条
          scroll={scrollConfigState}
          footer={() => (
            <div style={{ textAlign: 'center' }}>
              <a
                onClick={() => {
                  loadMoreOnClick();
                }}
                hidden={searchLoadMoreState}
              >
                加载更多...
              </a>
            </div>
          )}
          loading={searchLoadingState}
          options={false}
          search={false}
          pagination={false}
          rowKey={(record) => String(record.key)}
          onRow={(record) => {
            return {
              onClick: () => searchRecordOnSelect(record),
            };
          }}
        />
      </ProCard>
    </div>
  );
};

export default DcSearcher;
