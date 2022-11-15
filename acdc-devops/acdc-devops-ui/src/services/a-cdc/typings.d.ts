// @ts-ignore
/* eslint-disable */

declare namespace API {

	type LoginGuider = {
		url?: string;
		token?: string;
		user?: CurrentUser;
	}

	type CurrentUser = {
		userid?: string;
		email?: string;
		domainAccount?: string;
		username?: string;
		authorities?: Authority[];
	};

	type Authority = {
		authority?: string;
	}

	type LoginParams = {
		username?: string;
		password?: string;
		loginUrl?: string;
		loginSuccessUrl?: string;
	}

	type LogoutParams = {
		logoutSuccessUrl?: string;
	}

	/**
		-----------------------
		数据流 model
		-----------------------
	*/

	type ConnectorModel = {
		connectorId?: number;
		connectorName?: string;
		connectorType?: string;
		sourceDataSystemType?: string;
		sinkDataSystemType?: string;
		sourceConnectorInfo?: SourceConnectorInfo;
		sinkConnectorInfo?: SinkConnectorInfo;
		connectorConfig?: Map<string, string>;
		showDetail?: boolean;
		showEdit?: boolean;
		// 强制刷新使用
		refreshVersion?: number = 0;
	}

	type ConnectionModel = {
		connectionId?: number;
		connectionName?: string;
		sourceDataSystemType?: string;
		sinkDataSystemType?: string;
		showDetail?: boolean;
		showEdit?: boolean;
		// 强制刷新使用
		refreshVersion?: number = 0;
	}


	type ConnectorApplyModel = {
		// source
		//project
		srcPrjId?: number;
		srcPrjName?: string;
		srcSearchPrj?: string;
		srcPrjOwnerEmail?: string;
		//srcSearchCluster
		srcDataSystemType?: string;
		srcClusterId?: number;
		srcClusterType?: string;
		srcClusterName?: string;
		srcSearchCluster?: string;
		//src database
		srcDatabaseId?: number;
		srcDatabaseName?: string;
		srcSearchDatabase?: string;
		//src dataset
		srcDataSetId?: number;
		srcDataSetName?: string;
		srcSearchDataset?: string;
		// sink
		sinkPrjId?: number;
		sinkPrjName?: string;
		sinkSearchPrj?: string;
		//sink cluster
		sinkDataSystemType?: string;
		sinkClusterId?: number;
		sinkClusterType?: string;
		sinkClusterName?: string;
		sinkSearchCluster?: string;
		//sink instance
		sinkInstanceId?: number;
		sinkInstanceName?: string;
		sinkSearchInstance?: string;
		//sink database
		sinkDatabaseId?: number;
		sinkDatabaseName?: string;
		sinkSearchDatabase?: string;
		//sink dataset
		sinkDataSetId?: number;
		sinkDataSetName?: string;
		sinkSearchDataSet?: string;
		//
		sinkKafkaConverterType?: string;
		specificConfiguration?: string;
		// field mapping
		fieldMappings?: FieldMappingListItem[];
	}

	type FieldMappingModel = {
		// 来源 'apply', 'edit'
		from?: string;
		connectorId?: number;

		srcDataSystemType?: string;
		srcClusterType?: string;
		srcDataSetId?: number;

		sinkDataSystemType?: string;
		sinkClusterType?: string;
		sinkDataSetId?: number;

		fieldMappings?: FieldMappingListItem[];
	}

	type ConnectionFieldMappingModel = {
		// 来源 'Detail', 'apply', 'edit'
		from?: string;
		connectionId?: number;

		srcDataSystemType?: string;
		srcDataSetId?: number;

		sinkDataSystemType?: string;

		sinkDataSetId?: number;

		fieldMappings?: FieldMappingListItem[];
	}

	// project
	type ProjectEditingModel = {
		showDrawer?: boolean;
		projectId?: number;
		from?: string;
	}

	type ProjectConfigModel = {
		showDrawer?: boolean;
	}

	type ProjectDetailModel = {
		projectId?: number;
		name?: string;
		description?: string;
		ownerEmail?: string;
	}

	type ProjectUserModel = {
		projectId?: number;
	}

	// rdb
	type RdbClusterConfigModel = {
		rdbId?: number;
		showDrawer?: boolean;
	}

	type RdbClusterEditingModel = {
		showDrawer?: boolean
		from?: string;
		rdbId?: number;
		projectId?: number;
	}

	type RdbClusterDetailModel = {
		rdbId?: number;
		name?: string;
		desc?: string;
		username?: string;
		rdbType?: string;
	}

	type RdbClusterMgtModel = {
		projectId?: number
	}

	type RdbInstanceModel = {
		rdbId?: number
	}

	type RdbDatasetModel = {
		rdbId?: number
		databaseId?: number;
	}

	// kafka
	type KafkaClusterMgtModel = {
		projectId?: number
	}

	type KafkaClusterEditingModel = {
		showModal?: boolean
		from?: string;
		kafkaClusterId?: number;
		projectId?: number;
	}

	type KafkaClusterConfigModel = {
		kafkaClusterId?: number;
		showDrawer?: boolean;
	}

	type KafkaClusterDetailModel = {
		kafkaClusterId?: number;
	}

	type KafkaDatasetModel = {
		kafkaClusterId?: number
	}

	type RdbMysqlModel = {
		rdbId?: number;
		host?: string;
		port?: string;
		rdbType?: string;
		source?: string;
		// 强制刷新使用
		refreshVersion?: number = 0;
	}

	type RdbTidbModel = {
		rdbId?: number;
		topic?: string;
		rdbType?: string;
		source?: string;
		// 强制刷新使用
		refreshVersion?: number = 0;
	}

	/**
		-----------------------
		项目
		-----------------------
	*/
	type Project = {
		id?: number;
		name?: string;
		description?: string;
		ownerEmail?: string;
		projectSourceType?: string;
	}

	type ProjectList = {
		data?: Project[];
		total?: number;
		success?: boolean;
	};

	type ProjectQuery = {
		name?: string
		current?: number;
		pageSize?: number;
	};

	/**
		-----------------------
		RDB集群
		-----------------------
	*/
	type ClusterListItem = {
		id?: number;
		name?: string;
		desc?: string;
		dataSystemType?: string;
		clusterType?: string;
	}

	type ClusterList = {
		data?: ClusterListItem[];
		total?: number;
		success?: boolean;
	};

	type ClusterQuery = {
		name?: string
		projectId?: number;
		current?: number;
		pageSize?: number;
	}

	/**
		-----------------------
		RDB实例
		-----------------------
	*/
	type InstanceListItem = {
		id?: number;
		name?: string;
		role?: string;
	}

	type InstanceList = {
		data?: InstanceListItem[];
		total?: number;
		success?: boolean;
	};

	type InstanceQuery = {
		id?: number;
		clusterId?: number;
		dataSystemType?: string;
		name?: string;
		current?: number;
		pageSize?: number;
	}

	/**
		-----------------------
		RDB数据库
		-----------------------
	*/
	type DatabaseListItem = {
		id?: number;
		name?: string;
	}

	type DatabaseList = {
		data?: DatabaseListItem[];
		total?: number;
		success?: boolean;
	};

	type DatabaseQuery = {
		name?: string
		clusterId?: number;
		dataSystemType?: string;
		current?: number;
		pageSize?: number;
	}

	/**
		-----------------------
		数据集
		-----------------------
	*/
	type TableListItem = {
		id?: number;
		name?: string;
	}

	type TableList = {
		data?: TableListItem[];
		total?: number;
		success?: boolean;
	};

	type TableQuery = {
		name?: string
		databaseId?: number;
		dataSystemType?: string;
		current?: number;
		pageSize?: number;
	}

	/**
	-----------------------
	kafka
	-----------------------
*/

	type KafkaTopic = {
		id?: number;
		name?: string;
	}

	type KafkaTopicList = {
		data?: KafkaTopic[];
		total?: number;
		success?: boolean;
	}

	type KafkaConvertListItem = {
		id?: number;
		name?: string;
		convert?: string;
	}

	type KafkaConvertList = {
		data?: KafkaConvertListItem[];
		total?: number;
		success?: boolean;
	}

	type KafkaCluster = {
		id?: number;
		name?: string;
		version?: string;
		bootstrapServers?: string;
		description?: string;
		securityProtocol?: string;
		saslMechanism?: string;
		saslUsername?: string;
		saslPassword?: string;
		clusterType?: string;
		projectId?: number;
	}

	/**
		-----------------------
		字段映射
		-----------------------
	*/
	type Field = {
		title?: string;
		name?: string;
		dataType?: string;
		fieldKeyType?: string,
		allowNull?: string;
		defaultValue?: string;
		extra?: string;
	}

	type FieldMappingListItem = {
		id?: number;
		sourceFieldFormat?: string;
		sinkFieldFormat?: string;
		rowFilterExpress?: string;
		matchStatus?: int;
		filterOperator?: string;
		filterValue?: string;
		srcDataSetId?: number;
	}

	type FieldMappingItem = {
		sourceField?: Field;
		sinkField?: Field;
		rowFilterExpress?: string;
	}

	type FieldMappingList = {
		data?: FieldMappingListItem[];
		total?: number;
	};

	type FieldMappingQuery = {
		from?: string;
		connectorId?: number;
		srcDataSystemType?: string;
		srcDataSetId?: number;

		sinkDataSetId?: number;
		sinkDataSystemType?: string;
	}

	/**
	-----------------------
	用户
	-----------------------
	*/

	type AcdcUser = {
		id?: number;
		login?: string;
		email?: string;
		ownerFlag?: number;
	}

	type AcdcUserList = {
		data?: AcdcUser[];
		total?: number;
		success?: boolean;
	};

	/**
		-----------------------
		Connector 申请
		-----------------------
	*/

	type ConnectorApplyInfo = {
		sourceDataSystemType?: string;
		sinkDataSystemType?: string;
		sourceCreationInfo: SourceApplyInfo;
		sinkCreationInfo: SinkCreationInfo;
		fieldMappings?: FieldMappingListItem[];
	};

	type SourceCreationInfo = {
		clusterId?: number;
		instanceId?: number;
		databaseId?: number;
		dataSetId?: number;
	};
	type SinkCreationInfo = {
		clusterId?: number;
		instanceId?: number;
		databaseId?: number;
		dataSetId?: number;
		kafkaConverterType?: string;
	};


	/**
		-----------------------
		Connector 列表
		-----------------------
	*/
	type ConnectorListItem = {
		id?: number;
		name?: string;
		creator?: string;
		creationTime?: string;
		updateTime?: string
		desiredState?: string;
		actualState?: string;
		connectorType?: string;
		dataSystemType?: string;
	}

	type ConnectorList = {
		data?: ConnectorListItem[];
		total?: number;
		success?: boolean;
	};

	type ConnectorQuery = {
		refreshVersion?: number;
		name?: string
		current?: number;
		pageSize?: number;
	};

	type ConnectorEditInfo = {
		connectorId?: number;
		fieldMappings?: FieldMappingListItem[];
	}


	/**
	-----------------------
	Connection
	-----------------------
 */

	type Connection = {
		// basic info
		connectionId?: number;

		sourceDataSystemType?: string;
		sinkDataSystemType?: string;

		desiredState?: string;

		actualState?: string;

		requisitionState?: string;

		creationTimeFormat?: string;

		updateTimeFormat?: string;

		sourcePath?: string;
		sinkPath?: string;

		// id info
		sourceProjectId?: number;
		sinkProjectId?: number;

		sourceDatasetClusterId?: number;
		sinkDatasetClusterId?: number;

		sourceDatasetDatabaseId?: number;
		sinkDatasetDatabaseId?: number;

		sourceDatasetId?: number;
		sinkDatasetId?: number;

		sinkDatasetInstanceId?: number;

		// name info
		sourceProjectName?: string;
		sinkProjectName?: string;

		sourceDatasetClusterName?: string;
		sinkDatasetClusterName?: string;

		sourceDatasetDatabaseName?: string;
		sinkDatasetDatabaseName?: string;

		sourceDatasetName?: string;
		sinkDatasetName?: string;

		sinkDatasetInstanceName?: string;

		sourceConnectorId?: number;
		sinkConnectorId?: number;

	}

	type ConnectionList = {
		data?: Connection[];
		total?: number;
		success?: boolean;
	};

	type ConnectionEditInfo = {
		connectionId?: number;
		fieldMappings?: FieldMappingListItem[];
	}

	type ConnectionDetail = {
		// basic info
		id?: number;
		desiredState?: string;
		actualState?: string;
		requisitionState?: string;
		creationTimeFormat?: string;
		updateTimeFormat?: string;
		version?: number;
		specificConfiguration?: string;
		userEmail?: string;
		deleted?: boolean;

		// source 相关
		sourceDataSystemType?: string;
		// source connector
		sourceConnectorId?: number;
		sourceConnectorName?: string;
		// source 所属项目
		sourceProjectId?: number;
		sourceProjectName?: string;
		// source 数据系统
		sourceDataSystemClusterId?: number;
		sourceDataSystemClusterName?: string;
		// source 数据库
		sourceDatabaseId?: number;
		sourceDatabaseName?: string;
		// source 数据集
		sourceDatasetId?: number;
		sourceDatasetName?: string;

		// sink 相关
		sinkDataSystemType?: string;
		// sink connector
		sinkConnectorId?: number;
		sinkConnectorName?: string;
		// sink 所属项目
		sinkProjectId?: number;
		sinkProjectName?: string;
		// sink 数据系统
		sinkDataSystemClusterId?: number;
		sinkDataSystemClusterName?: string;
		// sink 数据库
		sinkDatabaseId?: number;
		sinkDatabaseName?: string;
		// sink dataset
		sinkDatasetId?: number;
		sinkDatasetName?: string;
		// sink instance
		sinkInstanceId?: number;
		sinkInstanceHost?: string;
		sinkInstanceVip?: string;
		sinkInstancePort?: number;

		connectionColumnConfigurations?: FieldMappingListItem[];
	}

	/**
	 * 暂未用到，后续将 ConnectionDetail.connectionColumnConfigurations 替换为此类型
	 */
	type ConnectionColumnConfiguration = {
		id?: number;
		connectionId?: number;
		connectionVersion?: number;
		sourceColumnName?: string;
		sinkColumnName?: string;
		filterOperator?: string;
		filterValue?: string;
		creationTimeFormat?: string;
		updateTimeFormat?: string;
	}

	/**
		-----------------------
		链路申请
		-----------------------
	*/

	type ConnectionRequisitionList = {
		data?: ConnectionRequisition[];
		total?: number;
		success?: boolean;
	};

	type ConnectionRequisition = {
		id?: number;
		state?: string;
		description?: string;
		sourceApproveResult?: string;
		sourceApproverEmail?: string;
		dbaApproveResult?: string;
		dbaApproverEmail?: string;
		updateTimeFormat?: string;
		creationTimeFormat?: string;
	}

	/**
		-----------------------
		链路详情
		-----------------------
	*/
	type KafkaTopic = {
		name?: string
	}

	type Connector = {
		id?: number;
		name?: string,
		creationTime?: string,
		updateTime?: string,
		desiredState?: string,
		actualState?: string,
	}

	/**
		-----------------------
		Rdb
		-----------------------
	*/

	type Rdb = {
		id?: number;
		name?: string;
		rdbType?: string;
		creationTime?: string;
		updateTime?: string;
		description?: string;
		desc?: string;
		projectId?: number;
		username?: string;
		password?: string;
	}

	type RdbListItem = {
		id?: number;
		name?: string;
		desc?: string;
		rdbType?: string;
		creationTime?: string,
		updateTime?: string,
	}

	type RdbList = {
		data?: RdbListItem[];
		total?: number;
		success?: boolean;
	};

	type RdbQuery = {
		id?: number;
		name?: string;
		rdbType?: string;
		current?: number;
		refreshVersion?: number;
		rdbType?: string;
		pageSize?: number;
	}

	type RdbMysqlEdit = {
		rdbId?: number;
		host?: string;
		port?: string;
	}

	type RdbTidbEdit = {
		rdbId?: number;
		topic?: string;
	}

	/**
		-----------------------
		Rdb database
		-----------------------
 */

	type RdbDatabase = {
		id?: number,
		name?: string,
	}


	/**
		-----------------------
		Rdb table
		-----------------------
 */
	type RdbTable = {
		id?: number
		name?: string,
	}

	type RdbInstance = {
		id?: number;
		host?: string;
		port?: number;
		roleType?: string;
	}

	/**
		-----------------------
		Hive
		-----------------------
	*/

	type Hive = {
		id?: number;
		name?: string;
	}

	type HiveDatabase = {
		id?: number;
		name: string;
	}

	type HiveTable = {
		id?: number;
		name?: string;
	}

	type SourceConnectorInfo = {
		connectorId?: number;
		name?: string;
		kafkaTopic?: string;
		srcDataSystemType?: string;
		srcCluster?: string;
		srcDatabase?: string;
		srcDataSet?: string;
	}

	type SinkConnectorInfo = {
		id?: number;
		name?: string;
		kafkaTopic?: string;
		sinkCluster?: string;
		sinkClusterType?: string;
		sinkDataSystemType?: string;
		sinkDatabase?: string;
		sinkDataSet?: string;
		srcCluster?: string;
		srcDatabase?: string;
		srcDataSet?: string;
		srcDataSetId?: number;
		srcDataSystemType?: string;
	}

	type SinkConnectorListItem = {
		id?: number;
		name?: string;
		kafkaTopic?: string;
		cluster?: string;
		database?: string;
		dataSet?: string;
	}

	type SinkConnectorList = {
		data?: SinkConnectorListItem[];
		total?: number;
		success?: boolean;
	};

	type SinkConnectorQuery = {
		sourceConnectorId?: number;
		sinkDataSystemType?: string;
		current?: number;
		pageSize?: number;
	}
	type ConnectorMgtEventList = {
		data?: ConnectorMgtEventItem[];
		total?: number;
		success?: boolean;
	};
	type ConnectorMgtEventItem = {
		id: number;
		reason?: string;
		message?: string;
		creationTime?: string;
		updateTime?: string;
		source: number;//0: 用户操作, 1: scheduler
		level: number;//0: normal, 1: trace, 2: warning, 3: error, 4: critical
	}
	type EventListQueryparams = {
		current?: number;
		pageSize?: number;
		reason?: string
		beginTime?: string
		endTime?: string
		level?: number;
		source?: number;
		connectorId?: number;
	}

	/**
	 * 审批相关
	 */
	type ConnectionRequisition = {

	}

	type ConnectionRequisitionDetail = {
		id?: number;
		sourceApproveResult?: string;
		dbaApproveResult?: string;
		sourceApproverEmail?: string;
		dbaApproverEmail?: string;
		state?: string;
		description?: string;
		connections?: ConnectionDetail[];
	}
}
