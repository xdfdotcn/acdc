// @ts-ignore
/CONSTANT* eslint-disable */;

import { request } from 'umi';

/**
 ***********************A CDC API********************************
 */

/**
 * Login
 */
export async function login(options?: { [key: string]: any }) {
  if (options == undefined) {
    options = {};
  }
  return request<{
    data: API.LoginGuider;
  }>('/api/login', {
    method: 'POST',
    params: {
      ...options,
    },
    ...(options || {}),
  });
}

/**
 * Logout
 */
export async function logout(options?: { [key: string]: any }) {
  return request<{
    data: API.LoginGuider;
  }>('/api/logout', {
    method: 'POST',
    params: {
      ...options,
    },
    ...(options || {}),
  });
}

/**
 * Get UI config
 */
export async function getUiConfig(options?: { [key: string]: any }) {
  let url = '/api/v1/ui/config';
  return request<{
    data: Map<String, String>;
  }>(url, {
    method: 'GET',
    ...(options || {}),
  });
}

/**
 * 查询项目列表,分页
 */
export async function pagedQueryProject(query: API.ProjectQuery, options?: { [key: string]: any }) {
  if (typeof query.deleted === 'undefined') {
    query.deleted = false;
  }

  return request<API.ProjectPageList>('/api/v1/projects', {
    method: 'GET',
    params: {
      ...query,
    },
    ...(options || {}),
  });
}

/**
 * 获取项目
 */
export async function getProject(
  params: {
    projectId?: number;
  },
  options?: { [key: string]: any },
) {
  if (!params || !params.projectId) {
    return {};
  }
  let url = '/api/v1/projects/' + params.projectId;
  return request(url, {
    method: 'GET',
    params: {
      ...params,
    },
    ...(options || {}),
  });
}

/**
 * 创建项目
 */
export async function createProject(body: API.Project, options?: { [key: string]: any }) {
  return request('/api/v1/projects', {
    method: 'POST',
    data: body,
    ...(options || {}),
  });
}

/**
 * 编辑项目
 */
export async function editProject(body: API.Project, options?: { [key: string]: any }) {
  return request('/api/v1/projects', {
    method: 'PATCH',
    data: body,
    ...(options || {}),
  });
}

/**
 * 查询项目用户
 */
export async function queryProjectUser(
  params: {
    // query
    /** 当前的页码 */
    current?: number;
    /** 页面的容量 */
    pageSize?: number;

    projectId?: number;
  },
  options?: { [key: string]: any },
) {
  if (!params || !params.projectId) {
    return [];
  }
  let url = '/api/v1/projects/' + params.projectId + '/users';
  return request<API.AcdcUser>(url, {
    method: 'GET',
    params: {
      ...params,
    },
    ...(options || {}),
  });
}

/**
 * 创建项目
 */
export async function editProjectUser(body: API.AcdcUser[], options?: { [key: string]: any }) {
  if (!body || !options || !options.projectId) {
    return;
  }

  let url = '/api/v1/projects/' + options.projectId + '/users';
  return request(url, {
    method: 'POST',
    data: body,
    ...(options || {}),
  });
}

/**
 * 查询所有用户信息
 */
export async function queryUser(
  params: {
    // query
    /** 当前的页码 */
    current?: number;
    /** 页面的容量 */
    pageSize?: number;
  },
  options?: { [key: string]: any },
) {
  return request<API.AcdcUser[]>('/api/v1/users', {
    method: 'GET',
    params: {
      ...params,
    },
    ...(options || {}),
  });
}

/**
 * 获取字段配置列表
 */
export async function generateConnectionColumnConf(
  sourceDataCollectionId: number,
  sinkDataCollectionId: number,
) {
  let url = '/api/v1/connections/column-configurations/generate';
  return request<API.ConnectionColumnConf[]>(url, {
    method: 'GET',
    params: {
      sourceDataCollectionId: sourceDataCollectionId,
      sinkDataCollectionId: sinkDataCollectionId,
    },
  });
}

export async function generateConnectionColumnConfByConnectionId(connectionId: number) {
  let url = '/api/v1/connections/' + connectionId + '/column-configurations/generate';
  return request<API.ConnectionColumnConf[]>(url, {
    method: 'GET',
    params: {},
  });
}

/**
 * 获取数据集字段声明
 */
export async function getDataCollectionDefinition(options?: { [key: string]: any }) {
  if (!options?.id) {
    return [];
  }
  let url = '/api/v1/data-system-resources/' + options?.id + '/data-collection-definition';
  return request<API.DataCollectionDefinition>(url, {
    method: 'GET',
    ...(options || {}),
  });
}

/**
 * 申请链路
 *
 */
export async function applyConnection(
  body: API.ConnectionRequisitionDetail,
  options?: { [key: string]: any },
) {
  return request('/api/v1/connections', {
    method: 'POST',
    data: body,
    ...(options || {}),
  });
}

/**
 * 更新链路
 */
export async function updateConnection(
  body: API.ConnectionDetail[],
  options?: { [key: string]: any },
) {
  if (body.length <= 0) {
    return;
  }

  let url = '/api/v1/connections';

  return request(url, {
    method: 'PATCH',
    data: body,
    ...(options || {}),
  });
}

/**
 * start a connector
 */
export async function startConnector(connectorId: number) {
  let url = '/api/connectors/' + connectorId + '/start';
  return request<any>(url, {
    method: 'POST',
  });
}

/**
 * start a connector
 */
export async function stopConnector(connectorId: number) {
  let url = '/api/connectors/' + connectorId + '/stop';
  return request<any>(url, {
    method: 'POST',
  });
}

/**
 * start a connection
 */
export async function startConnection(connectionId: number) {
  let url = '/api/v1/connections/' + connectionId + '/start';
  return request<any>(url, {
    method: 'POST',
  });
}

/**
 * start a connection
 */
export async function stopConnection(connectionId: number) {
  let url = '/api/v1/connections/' + connectionId + '/stop';
  return request<any>(url, {
    method: 'POST',
  });
}

/**
 * 获取 sink 的实际状态
 */
export async function getConnectionActualStatus(options?: { [key: string]: any }) {
  if (!options?.id) {
    return;
  }

  let url = '/api/v1/connections/' + options?.id + '/actualStatus';
  return request<string>(url, {
    method: 'GET',
    ...(options || {}),
  });
}

/**
 * 查询 connector 列表
 */
export async function queryConnectors(
  params: {
    // 增加这个参数就是为了触发状态修改请求数据
    current?: number;
    pageSize?: number;
    refreshVersion?: number;
  },
  options?: { [key: string]: any },
) {
  return request<API.ConnectorList>('/api/connectors', {
    method: 'GET',
    params: {
      ...params,
    },
    ...(options || {}),
  });
}

/**
 * 查询 connection 列表
 */
export async function queryConnection(
  params: {
    // 增加这个参数就是为了触发状态修改请求数据
    current?: number;
    pageSize?: number;
    refreshVersion?: number;
  },
  options?: { [key: string]: any },
) {
  console.log(options);
  return request<API.ConnectionList>('/api/v1/connections', {
    method: 'GET',
    params: {
      ...params,
      deleted: false,
    },
    ...(options || {}),
  });
}

/**
 * 删除 connection
 */
export async function deleteConnection(options?: { [key: string]: any }) {
  if (!options?.connectionId) {
    return [];
  }
  let url = '/api/v1/connections/' + options?.connectionId;

  return request(url, {
    method: 'DELETE',
    params: {},
    ...(options || {}),
  });
}

/**
 * 获取 conection 详情
 */
export async function getConnectionDetail(id: number) {
  if (!id) {
    return;
  }

  const url = '/api/v1/connections/' + id;

  return request<API.ConnectionDetail>(url, {
    method: 'GET',
  });
}

/**获取 sink 详情 */
export async function getConnectorDetail(id: number) {
  if (!id) {
    return;
  }

  const url = '/api/connectors/' + id;
  return request<API.ConnectorDetail>(url, {
    method: 'GET',
  });
}

/**
 * 查询  connector event列表
 */
export async function getEventList(
  connectorId: number,
  params: API.EventListQueryparams,
  options?: { [key: string]: any },
) {
  return request<API.ConnectorMgtEventList>(`/api/v1/connectors/${connectorId}/events`, {
    method: 'GET',
    params: {
      ...params,
    },
    ...(options || {}),
  });
}

/**
 * 获取申请单详情
 */
export async function getConnectionRequisitionDetail(id: number) {
  if (!id) {
    return;
  }

  const url = '/api/v1/connection-requisition/' + id;

  return request<API.Connection>(url, {
    method: 'GET',
  });
}

/**
 * 申请单审批
 */
export async function doConnectionRequisitionApprove(
  id: number,
  approved: boolean,
  approveResult: string,
) {
  if (!id) {
    return;
  }

  const url = '/api/v1/connection-requisitions/' + id + '/approve';

  return request<API.Connection>(url, {
    method: 'POST',
    data: {
      approveResult: approveResult,
      approved: approved,
    },
  });
}

/**
 * 查询申请单列表
 */
export async function queryConnectonRequisition(
  params: {
    // 增加这个参数就是为了触发状态修改请求数据
    connectionId?: number;
    current?: number;
    pageSize?: number;
  },
  options?: { [key: string]: any },
) {
  if (!params.connectionId) {
    return [];
  }
  let url = '/api/v1/connections/' + params.connectionId + '/connection-requisitions';
  return request(url, {
    method: 'GET',
    params: {
      ...params,
    },
    ...(options || {}),
  });
}

/**
 * 数据系统资源列表查询(分页)
 */
export async function getDataSystemResource(
  params: {
    id?: number;
  },
  options?: { [key: string]: any },
) {
  let url = '/api/v1/data-system-resources/' + params.id;
  return request<API.DataSystemResource>(url, {
    method: 'GET',
    params: {
      ...params,
    },
    ...(options || {}),
  });
}

/**
 * 数据系统资源列表查询(分页)
 */
export async function pagedQueryDataSystemResource(
  query: API.DataSystemResourceQuery,
  options?: { [key: string]: any },
) {
  if (typeof query.deleted === 'undefined') {
    query.deleted = false;
  }

  let requestParams = { ...query };

  if (query.resourceConfigurations) {
    let resourceConfigurations = query.resourceConfigurations;
    let resourceConfigurationsKey = 'resourceConfigurations';

    delete requestParams.resourceConfigurations;

    for (let key in resourceConfigurations) {
      requestParams[resourceConfigurationsKey + '[' + key + ']'] = resourceConfigurations[key];
    }
  }

  let url = '/api/v1/data-system-resources';
  return request<API.DataSystemResourcePageList>(url, {
    method: 'GET',
    params: {
      ...requestParams,
    },
    ...(options || {}),
  });
}

/**
 * 编辑数据系统资源
 */
export async function updateDataSystemResource(
  body: API.DataSystemResource,
  options?: { [key: string]: any },
) {
  let url = '/api/v1/data-system-resources/' + body.id;

  return request(url, {
    method: 'PUT',
    data: body,
    ...(options || {}),
  });
}

/**
 * 更新某个父节点的子节点
 */
export async function updateChildDataSystemResources(
  parentResourceId: number,
  body: API.DataSystemResource[],
  options?: { [key: string]: any },
) {
  let url = '/api/v1/data-system-resources/' + parentResourceId + '/data-system-resources';

  return request(url, {
    method: 'POST',
    data: body,
    ...(options || {}),
  });
}

/**
 * 创建数据系统资源
 */
export async function createDataSystemResource(
  body: API.DataSystemResource,
  options?: { [key: string]: any },
) {
  let url = '/api/v1/data-system-resources/';
  return request(url, {
    method: 'POST',
    data: body,
    ...(options || {}),
  });
}

/**
 * 刷新数据系统
 */
export async function refreshDynamicDataSystemResource(
  resourceId: number,
  options?: { [key: string]: any },
) {
  let url = '/api/v1/data-system-resources/' + resourceId + '/refresh';
  return request(url, {
    method: 'POST',
    ...(options || {}),
  });
}

/**
 * 数据系统资源列表查询(分页)
 */
export async function queryDataSystemResourceDefinition() {
  let url = '/api/v1/data-system-resources/definitions';
  return request<API.DataSystemResourceDefinition[]>(url, {
    method: 'GET',
    params: {},
    ...{},
  });
}

export async function validateDataCollection(body: number[], options?: { [key: string]: any }) {
  let url = '/api/v1/biz/data-system-resources/data-collection/validate';

  return request<boolean>(url, {
    method: 'POST',
    data: body,
    ...(options || {}),
  });
}

/**
 * 查询 wide table 列表
 */
export async function queryWideTable(
  params: {
    // 增加这个参数就是为了触发状态修改请求数据
    current?: number;
    pageSize?: number;
    refreshVersion?: number;
  },
  options?: { [key: string]: any },
) {
  console.log(options);
  return request<API.WideTableList>('/api/v1/wide-table', {
    method: 'GET',
    params: {
      ...params,
      deleted: false,
    },
    ...(options || {}),
  });
}

/**
 * disable a wide table
 */
export async function disableWideTable(wideTableId: number) {
  let url = '/api/v1/wide-table/' + wideTableId + '/disable';
  return request<any>(url, {
    method: 'POST',
  });
}

/**
 * enable a wide table
 */
export async function enableWideTable(wideTableId: number) {
  let url = '/api/v1/wide-table/' + wideTableId + '/enable';
  return request<any>(url, {
    method: 'POST',
  });
}

/**
 * 获取 wide table 详情
 */
export async function getWideTableDetail(id: number) {
  if (!id) {
    return;
  }

  const url = '/api/v1/wide-table/' + id;

  return request<API.WideTableDetail>(url, {
    method: 'GET',
  });
}

/**
 * 获取 wide table 关联的 connections
 */
export async function getWideTableConnections(wideTableId: number) {
  const url = '/api/v1/wide-table/' + wideTableId + '/connections';

  const response = await fetch(url);
  return await response.json();
}

/**
 * 获取 wide table 的审批记录
 */
export async function getWideTableRequisitionBatch(wideTableId: number) {
  const url = '/api/v1/wide-table/' + wideTableId + '/requisition';

  const response = await fetch(url);
  return await response.json();
}

/**
 * 创建宽表.
 */
export async function createWideTable(
  body: API.WideTableDetail,
  options?: { [key: string]: any },
) {
  let url = '/api/v1/wide-table?beforeCreation=false';
  return request(url, {
    method: 'POST',
    data: body,
    ...(options || {}),
  });
}
