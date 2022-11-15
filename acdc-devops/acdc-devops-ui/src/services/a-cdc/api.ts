// @ts-ignore
/CONSTANT* eslint-disable */
import {conditionalExpression} from '@babel/types';

import {history, request} from 'umi';

/**
***********************A CDC API********************************
*/

/** e2 login */
export async function e2Login(options?: {[key: string]: any}) {
	return request<{
		data: API.CurrentUser
	}>('/e2/login', {
		method: 'GET',
		...(options || {}),
	});
}

/** e2 login */
export async function login(options?: {[key: string]: any}) {
	if (options == undefined) {
		options = {};
	}
	return request<{
		data: API.LoginGuider
	}>('/login', {
		method: 'POST',
		params: {
			...options
		},
		...(options || {}),
	});
}

export async function logout(options?: {[key: string]: any}) {
	return request<{
		data: API.LoginGuider
	}>('/logout', {
		method: 'POST',
		params: {
			...options
		},
		...(options || {}),
	});
}

/** Get UI config */
export async function getUiConfig(
	options?: {[key: string]: any},
) {
	let url = '/api/v1/ui/config';
	return request<{
		data: Map<String, String>
	}>(url, {
		method: 'GET',
		...(options || {}),
	});
}


/** 查询项目列表 */
export async function queryProject(
	params: {
		// query
		/** 当前的页码 */
		from?: string;
		current?: number;
		/** 页面的容量 */
		pageSize?: number;
	},
	options?: {[key: string]: any},
) {
	return request<API.ProjectList>('/api/projects', {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}


/** 获取项目 */
export async function getProject(
	params: {
		projectId?: number;
	},
	options?: {[key: string]: any},
) {

	if (!params || !params.projectId) {
		return {}
	}
	let url = '/api/projects/' + params.projectId;
	return request(url, {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

/** 创建项目 */
export async function createProject(body: API.Project, options?: {[key: string]: any}) {
	return request('/api/projects', {
		method: 'POST',
		data: body,
		...(options || {}),
	});
}

/** 编辑项目 */
export async function editProject(body: API.Project, options?: {[key: string]: any}) {
	return request('/api/projects', {
		method: 'PATCH',
		data: body,
		...(options || {}),
	});
}


/** 查询项目用户 */
export async function queryProjectUser(
	params: {
		// query
		/** 当前的页码 */
		current?: number;
		/** 页面的容量 */
		pageSize?: number;

		projectId?: number;
	},
	options?: {[key: string]: any},
) {

	if (!params || !params.projectId) {
		return []
	}
	let url = '/api/v1/projects/' + params.projectId + '/users'
	return request(url, {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

/** 创建项目 */
export async function editProjectUser(body: API.AcdcUser[], options?: {[key: string]: any}) {

	if (!body || !options || !options.projectId) {
		return
	}

	let url = '/api/v1/projects/' + options.projectId + '/users'
	return request(url, {
		method: 'POST',
		data: body,
		...(options || {}),
	});
}

/** 查询所有用户信息*/
export async function queryUser(
	params: {
		// query
		/** 当前的页码 */
		current?: number;
		/** 页面的容量 */
		pageSize?: number;
	},
	options?: {[key: string]: any},
) {
	return request<API.AcdcUser[]>('/api/v1/users', {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}



/** 查询数据库集群列表 */
export async function queryCluster(
	params: {
		projectId?: number;
		current?: number;
		pageSize?: number;
	},
	options?: {[key: string]: any},
) {

	// 校验,页面刷新或者无效id,返回空列表
	if (!params.projectId) {
		return [];
	}

	return request<API.ClusterList[]>('/api/clusters', {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

/** 查询数据库实例列表 */
export async function queryInstance(
	params: {
		clusterId?: number;
		dataSystemType?: string;
		name?: string;
		current?: number;
		pageSize?: number;
	},
	options?: {[key: string]: any},
) {

	if (!params.clusterId) {
		return [];
	}

	return request<API.InstanceList[]>('/api/instances', {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

/** 编辑 rdb 实例 */
export async function editRdbInstance(body: API.RdbInstance[], options?: {[key: string]: any}) {

	if (!body || !options || !options.rdbId) {
		return
	}

	let url = '/api/rdbs/' + options.rdbId + '/instances'
	return request(url, {
		method: 'POST',
		data: body,
		...(options || {}),
	});
}


/** 查询关系型数据库列表 */
export async function queryRdbInstance(
	params: {
		rdbId?: number;
		current?: number;
		pageSize?: number;
	},
	options?: {[key: string]: any},
) {
	let url = '/api/rdbs/' + params.rdbId + '/instances'
	return request(url, {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}


/** 查询数据库表列 */
export async function queryRdbDatabase(
	params: {
		clusterId?: number;
		current?: number;
		pageSize?: number;
	},
	options?: {[key: string]: any},
) {

	if (!params.clusterId) {
		return [];
	}
	return request<API.DatabaseList[]>('/api/databases/rdbDatabases', {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

export async function queryHiveDatabase(
	params: {
		clusterId?: number;
		current?: number;
		pageSize?: number;
	},
	options?: {[key: string]: any},
) {

	if (!params.clusterId) {
		return [];
	}
	return request<API.DatabaseList[]>('/api/databases/hiveDatabases', {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

export async function queryKafkaTopics(
	params: {
		name?: string;
		kafkaClusterId?: number;
		current?: number;
		pageSize?: number;
	},
	options?: {[key: string]: any},
) {

	if (!params.kafkaClusterId) {
		return [];
	}
	return request('/api/kafkaTopics', {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}



/** 查询数据表列表 */
export async function queryRdbTable(
	params: {
		databaseId?: number;
		dataSystemType?: string;
		current?: number;
		pageSize?: number;
	},
	options?: {[key: string]: any},
) {

	if (!params.databaseId) {
		return [];
	}
	return request<API.DatabaseList[]>('/api/tables/rdbTables', {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

export async function queryHiveTable(
	params: {
		databaseId?: number;
		dataSystemType?: string;
		current?: number;
		pageSize?: number;
	},
	options?: {[key: string]: any},
) {
	if (!params.databaseId) {
		return [];
	}
	return request<API.DatabaseList[]>('/api/tables/hiveTables', {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

/** 查询字段映射 */
export async function fetchFieldMapping(
	params: {
		from?: string;
		connectorId?: number;
		srcDataSystemType?: string;
		srcDataSetId?: number;
		sinkDataSetId?: number;
		sinkDataSystemType?: string;
	},
	options?: {[key: string]: any},
) {
	// 详情页面和修改页面,必须传入connectorId
	if ('apply' != params.from && !params.connectorId) {
		return []
	}

	// 解决上一个页面保留的历史状态问题,导致错误请求
	if (
		params.from != 'apply'
		&& history.location.pathname == '/connector/connectorApply') {
		return []
	}

	// 申请页面,必须指定上下游表,和下游数据库类型
	if (
		'apply' == params.from
		&& (!params.srcDataSetId
			|| !params.sinkDataSetId
			|| !params.sinkDataSystemType
			|| !params.srcDataSystemType
		)
	) {
		return [];
	}

	// 申请页面请求
	if ('apply' == params.from) {
		return request<API.FieldMappingList[]>('/api/fieldMappings', {
			method: 'GET',
			params: {
				...params,
			},
			...(options || {}),
		});
	}

	// 编辑页面,详情页面请求
	let url = '/api/fieldMappings/connectors/' + params?.connectorId;
	return request<string[]>(url, {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

/** 查询字段映射 */
export async function fetchConnectionFieldMapping(
	params: {
		current?: number;
		pageSize?: number;
		from?: string;
		connectionId?: number;
		srcDataSystemType?: string;
		srcDataSetId?: number;
		sinkDataSetId?: number;
		sinkDataSystemType?: string;
	},
	options?: {[key: string]: any},
) {

	// 详情页面和修改页面,必须传入connectorId
	if ('apply' != params.from && !params.connectionId) {
		return []
	}

	// 解决上一个页面保留的历史状态问题,导致错误请求
	if (
		params.from != 'apply'
		&& history.location.pathname == '/connection/connectionApply') {
		return []
	}

	// 申请页面,必须指定上下游表,和下游数据库类型
	if (
		'apply' == params.from
		&& (!params.srcDataSetId
			|| !params.sinkDataSetId
			|| !params.sinkDataSystemType
			|| !params.srcDataSystemType
		)
	) {
		return [];
	}

	// 申请页面请求
	if ('apply' == params.from) {
		return request<API.FieldMappingList[]>('/api/fieldMappings', {
			method: 'GET',
			params: {
				...params,
			},
			...(options || {}),
		});
	}

	// 编辑页面,详情页面请求
	// /connections/{connectionId}/connection-column-configurations-merged-with-current-ddl
	let url = '/api/connections/' + params?.connectionId + '/connection-column-configurations-merged-with-current-ddl';
	return request<string[]>(url, {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}


/** 查询数据表字段列表 */
export async function fetchDataSetFields(options?: {[key: string]: any}) {
	if (!options?.id) {
		return [];
	}
	let url = '/api/fieldMappings/' + options?.id + '/dataSetFields';
	return request<string[]>(url, {
		method: 'GET',
		...(options || {}),
	});
}

/** 申请connector */
export async function applyConnector(body: API.ConnectorApplyInfo, options?: {[key: string]: any}) {
	return request('/api/connectors', {
		method: 'POST',
		data: body,
		...(options || {}),
	});
}


export async function applyMultiConnector(body: API.ConnectorApplyInfo, options?: {[key: string]: any}) {
	return request('/api/v1/connections', {
		method: 'POST',
		data: body,
		...(options || {}),
	});
}

/** 修改 sink connector */
export async function editConnector(body: API.ConnectorEditInfo, options?: {[key: string]: any}) {
	if (!body?.connectorId) {
		return;
	}

	let url = '/api/connectors';

	return request(url, {
		method: 'PATCH',
		data: body,
		...(options || {}),
	});
}

export async function editConnection(body: API.ConnectorEditInfo[], options?: {[key: string]: any}) {
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


/** 更改connector 状态 */
export async function editConnectorStatus(
	// 如果增加parmas,会拦截options,目前这俩使用效果一样
	//params: {
	//connectorId?: number;
	//code?: string;
	//},
	options?: {[key: string]: any}
) {
	if (!options?.connectorId || !options?.state) {
		return
	}

	let url = '/api/connectors/' + options.connectorId + '/status';
	return request<any>(url, {
		method: 'PUT',
		params: {
			state: options.state
		},
		...(options || {}),
	});
}

export async function editConnectionStatus(
	// 如果增加parmas,会拦截options,目前这俩使用效果一样
	//params: {
	//connectorId?: number;
	//code?: string;
	//},
	options?: {[key: string]: any}
) {
	if (!options?.connectionId || !options?.state) {
		return
	}

	let url = '/api/v1/connections/' + options.connectionId + '/desiredStatus';
	return request<any>(url, {
		method: 'PUT',
		params: {
			state: options.state
		},
		...(options || {}),
	});
}

/**获取 sink 详情 */
export async function getConnectionActualStatus(options?: {[key: string]: any}) {
	if (!options?.id) {
		return
	}

	let url = '/api/v1/connections/' + options?.id + '/actualStatus';
	return request<string>(url, {
		method: 'GET',
		...(options || {}),
	});
}



/** 查询 connector 列表 */
export async function queryConnectors(
	params: {
		// 增加这个参数就是为了触发状态修改请求数据
		current?: number;
		pageSize?: number;
		refreshVersion?: number;
	},
	options?: {[key: string]: any},
) {
	return request<API.ConnectorList>('/api/connectors', {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

/** 查询 connector 列表 */
export async function queryConnection(
	params: {
		// 增加这个参数就是为了触发状态修改请求数据
		current?: number;
		pageSize?: number;
		refreshVersion?: number;
	},
	options?: {[key: string]: any},
) {
	return request<API.ConnectionList>('/api/v1/connections', {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}


/** 删除 connection */
export async function deleteConnection(
	options?: {[key: string]: any}
) {
	if (!options?.connectionId) {
		return [];
	}
	let url = '/api/v1/connections/' + options?.connectionId;

	return request<API.ConnectionList>(url, {
		method: 'DELETE',
		params: {
		},
		...(options || {}),
	});
}


/**获取 conection 详情*/
export async function getConnectionDetail(id: number) {
	if (!id) {
		return
	}

	let url = '/api/v1/connections/' + id;

	return request<API.ConnectionDetail>(url, {
		method: 'GET',
	});
}

/**获取 sink 详情 */
export async function getSinkConnectorInfo(options?: {[key: string]: any}) {
	if (!options?.id) {
		return
	}

	let url = '/api/connectors/sinks/' + options?.id;
	return request<API.SinkConnectorInfo>(url, {
		method: 'GET',
		...(options || {}),
	});
}


/** 获取source 链路详情 */
export async function getSourceConnectorInfo(options?: {[key: string]: any}) {
	if (!options?.id) {
		return
	}

	let url = '/api/connectors/sources/' + options?.id;
	return request<API.SourceConnectorInfo>(url, {
		method: 'GET',
		...(options || {}),
	});
}

/** 查询connector 配置*/
export async function getConnectorConfig(options?: {[key: string]: any}) {
	if (!options?.id) {
		return
	}

	let url = '/api/connectors/' + options?.id + '/config';
	return request<Map<string, string>>(url, {
		method: 'GET',
		...(options || {}),
	});
}

/** 查询 sink connector列表*/
export async function querySinks(
	// 1. params 的优先级更高,如果使用parmas,则options不起作用
	// 2. 取消掉params options也可以代替
	params: {
		sourceConnectorId?: number;
		sinkDataSystemType?: string;
		current?: number;
		pageSize?: number;
	},
	options?: {[key: string]: any},
) {
	if (!params.sourceConnectorId) {
		return []
	}
	let url = '/api/connectors/sinks';
	return request<API.SinkConnectorList>(url, {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

/** 查询 sink connector event列表*/
export async function getEventList(
	connectorId: number,
	params: API.EventListQueryparams,
	options?: {[key: string]: any},
) {
	return request<API.ConnectorMgtEventList>(`/api/v1/connectors/${connectorId}/events`, {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

export async function queryRdb(
	params: {
		// 增加这个参数就是为了触发状态修改请求数据
		name?: string;
		rdbType?: string;
		current?: number;
		pageSize?: number;
	},
	options?: {[key: string]: any},
) {
	if (!params.rdbType) {
		return []
	}
	return request<API.RdbList>('/api/rdbs', {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}


/** 获取RDB */
export async function getRdb(
	params: {
		rdbId?: number;
	},
	options?: {[key: string]: any},
) {

	if (!params || !params.rdbId) {
		return {}
	}
	let url = '/api/rdbs/' + params.rdbId;
	return request(url, {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

/** 创建RDB */
export async function createRdb(body: API.Rdb, options?: {[key: string]: any}) {
	return request('/api/rdbs', {
		method: 'POST',
		data: body,
		...(options || {}),
	});
}

/** 编辑RDB */
export async function editRdb(body: API.Rdb, options?: {[key: string]: any}) {
	return request('/api/rdbs', {
		method: 'PATCH',
		data: body,
		...(options || {}),
	});
}



/**查询项目下的RDB 列表*/
export async function queryProjectRdb(
	params: {
		// 增加这个参数就是为了触发状态修改请求数据
		projectId?: number;
		name?: string;
		current?: number;
		pageSize?: number;
	},
	options?: {[key: string]: any},
) {
	let url = '/api/projects/' + params.projectId + '/rdbs'
	return request(url, {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

/** 查询kafka集群列表 */
export async function queryKafkaCluster(
	params: {
		current?: number;
		/** 页面的容量 */
		pageSize?: number;

		projectId?: number;
	},
	options?: {[key: string]: any},
) {
	return request('/api/v1/kafka-clusters', {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

/** 获取项目 */
export async function getKafkaCluster(
	params: {
		kafkaClusterId?: number;
	},
	options?: {[key: string]: any},
) {

	if (!params || !params.kafkaClusterId) {
		return {}
	}
	let url = '/api/v1/kafka-clusters/' + params.kafkaClusterId;
	return request(url, {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

/** 创建项目 */
export async function createKafkaCluster(body: API.KafkaCluster, options?: {[key: string]: any}) {
	return request('/api/v1/kafka-clusters', {
		method: 'POST',
		data: body,
		...(options || {}),
	});
}

/** 编辑项目 */
export async function editKafkaCluster(body: API.KafkaCluster, options?: {[key: string]: any}) {
	return request('/api/v1/kafka-clusters', {
		method: 'PUT',
		data: body,
		...(options || {}),
	});
}



/**
 * 审批相关
 */
export async function getConnectionRequisitionDetail(id: number) {
	if (!id) {
		return
	}

	let url = '/api/v1/connection-requisition/' + id;

	return request<API.Connection>(url, {
		method: 'GET',
	});
}

export async function doConnectionRequisitionApprove(id: number, approved: boolean, approveResult: string) {
	if (!id) {
		return
	}

	let url = '/api/v1/connection-requisitions/' + id + '/approve';

	return request<API.Connection>(url, {
		method: 'POST',
		data: {
			approveResult: approveResult,
			approved: approved,
		}
	});
}

export async function queryConnectonRequisition(
	params: {
		// 增加这个参数就是为了触发状态修改请求数据
		connectionId?: number;
		current?: number;
		pageSize?: number;
	},
	options?: {[key: string]: any},
) {

	if (!params.connectionId) {
		return []
	}
	let url = '/api/v1/connections/' + params.connectionId + '/connection-requisitions';
	return request<API.RdbList>(url, {
		method: 'GET',
		params: {
			...params,
		},
		...(options || {}),
	});
}

