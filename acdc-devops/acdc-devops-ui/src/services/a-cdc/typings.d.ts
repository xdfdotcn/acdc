// @ts-ignore
/* eslint-disable */

declare namespace API {
  /**
   -----------------------
   用户
   -----------------------
   */
  type AcdcUser = {
    id?: number;
    login?: string;
    email?: string;
  };

  type LoginGuider = {
    url?: string;
    token?: string;
    user?: CurrentUser;
  };

  type CurrentUser = {
    userid?: string;
    email?: string;
    domainAccount?: string;
    username?: string;
    authorities?: Authority[];
  };

  type Authority = {
    authority?: string;
  };

  type LoginParams = {
    username?: string;
    password?: string;
    loginUrl?: string;
    loginSuccessUrl?: string;
  };

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
  };

  type ConnectionModel = {
    connectionId?: number;
    connectionName?: string;
    sourceDataSystemType?: string;
    sinkDataSystemType?: string;
    showDetail?: boolean;
    showEdit?: boolean;
    // 强制刷新使用
    refreshVersion?: number = 0;
  };

  type ConnectionApplyModel = {
    // source
    //project
    srcPrjId?: number;
    srcPrjName?: string;
    srcSearchPrj?: string;
    srcPrjOwnerName?: string;
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
    srcDataCollectionId?: number;
    srcDataCollectionName?: string;
    srcSearchDataCollection?: string;
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
    sinkDataCollectionId?: number;
    sinkDataCollectionName?: string;
    sinkSearchDataCollection?: string;
    //
    sinkKafkaConverterType?: string;
    specificConfiguration?: string;
    // field mapping
    columnConfList?: ConnectionColumnConf[];
  };

  type ConnectionColumnConfModel = {
    // 来源 'Detail', 'apply', 'edit'
    from?: string;

    connectionId?: number;

    sourceDataSystemType?: string;

    sourceDataCollectionId?: number;

    sinkDataSystemType?: string;

    sinkDataCollectionId?: number;

    // 表格展示数据
    displayData: ConnectionColumnConf[];

    // 原始表格数据
    originalData: ConnectionColumnConf[];

    // 当前表格数据
    currentData?: ConnectionColumnConf[];

    version?: number;

    canEdit: boolean;

    canDelete: boolean;
  };

  // project
  type ProjectEditingModel = {
    showDrawer?: boolean;
    projectId?: number;
    from?: string;
  };

  type ProjectConfigModel = {
    showDrawer?: boolean;
  };

  type ProjectDetailModel = {
    projectId?: number;
    name?: string;
    description?: string;
    ownerEmail?: string;
  };

  type ProjectUserModel = {
    projectId?: number;
    ownerEmail?: string;
  };

  type RdbClusterConfigModel = {
    resourceId?: number;
    dataSystemType?: string;
    showDrawer?: boolean;
  };

  type RdbClusterEditingModel = {
    showDrawer?: boolean;
    from?: string;
    resourceId?: number;
    projectId?: number;
  };

  type RdbClusterDetailModel = {
    resourceId?: number;
    name?: string;
    description?: string;
    username?: string;
    dataSystemType?: string;
  };

  type RdbClusterMgtModel = {
    projectId?: number;
  };

  type RdbInstanceModel = {
    resourceId?: number;
    dataSystemType?: string;
  };

  type RdbDatasetModel = {
    clusterResourceId?: number;
    databaseResourceId?: number;
    dataSystemType?: string;
  };

  type KafkaClusterMgtModel = {
    projectId?: number;
  };

  type KafkaClusterEditingModel = {
    showModal?: boolean;
    from?: string;
    kafkaClusterId?: number;
    projectId?: number;
  };

  type KafkaClusterConfigModel = {
    kafkaClusterId?: number;
    showDrawer?: boolean;
  };

  type KafkaClusterDetailModel = {
    kafkaClusterId?: number;
  };

  type KafkaDatasetModel = {
    kafkaClusterId?: number;
  };

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
    ownerName?: string;
    projectSourceType?: string;
  };

  type ProjectPageList = {
    data?: Project[];
    total?: number;
    success?: boolean;
  };

  type ProjectQuery = {
    name?: string;
    queryRange?: string;
    current?: number;
    pageSize?: number;
    deleted?: boolean;
  };

  /**
   -----------------------
   kafka
   -----------------------
   */

  type KafkaConvertListItem = {
    id?: number;
    name?: string;
    convert?: string;
  };

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
  };

  /**
   -----------------------
   connection 字段列配置
   -----------------------
   */
  type ConnectionColumnConf = {
    id?: string;
    rowId?: string;
    sourceColumnName?: string;
    sourceColumnType?: string;
    sourceColumnUniqueIndexNames?: string[];
    sinkColumnName?: string;
    sinkColumnType?: string;
    sinkColumnUniqueIndexNames?: string[];
    filterOperator?: string;
    filterValue?: string;
  };

  type ConnectionColumnConfList = {
    data?: ConnectionColumnConf[];
    total?: number;
    success?: boolean;
  };

  type FieldMappingQuery = {
    from?: string;
    connectorId?: number;
    srcDataSystemType?: string;
    srcDataSetId?: number;

    sinkDataSetId?: number;
    sinkDataSystemType?: string;
  };

  /**
   -----------------------
   conenctor
   -----------------------
   */
  type ConnectorListItem = {
    id?: number;
    name?: string;
    creator?: string;
    creationTime?: string;
    updateTime?: string;
    desiredState?: string;
    actualState?: string;
    connectorType?: string;
    dataSystemType?: string;
  };

  type ConnectorList = {
    data?: ConnectorListItem[];
    total?: number;
    success?: boolean;
  };

  type ConnectorQuery = {
    refreshVersion?: number;
    name?: string;
    current?: number;
    pageSize?: number;
  };

  type ConnectorEditInfo = {
    connectorId?: number;
    fieldMappings?: ConnectionColumnConf[];
  };

  /**
   -----------------------
   Connection
   -----------------------
   */

  type Connection = {
    // basic info
    id?: number;

    sourceDataSystemType?: string;
    sinkDataSystemType?: string;

    desiredState?: string;

    actualState?: string;

    requisitionState?: string;

    creationTime?: string;

    updateTime?: string;

    userEmail?: string;

    // id info
    sourceProjectId?: number;
    sinkProjectId?: number;

    sourceDataCollectionId?: number;
    sourceDataCollectionName?: string;
    sinkDataCollectionId?: number;
    sinkDataCollectionName?: string;

    // name info
    sourceProjectName?: string;
    sinkProjectName?: string;

    // data collection resource path
    sourceDataCollectionResourcePath?: DataSystemResource[];
    sinkDataCollectionResourcePath?: DataSystemResource[];

    sourceConnectorId?: number;
    sinkConnectorId?: number;
    sinkConnectorName?: string;

    sourceDataCollectionTopicName: number;
  };

  type ConnectionList = {
    data?: Connection[];
    total?: number;
    success?: boolean;
  };

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
    sourceDataCollectionId?: number;
    sourceDataCollectionName?: string;

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
    sinkDataCollectionId?: number;
    sinkDataCollectionName?: string;
    // sink instance
    sinkInstanceId?: number;
    sinkInstanceHost?: string;
    sinkInstanceVip?: string;
    sinkInstancePort?: number;

    connectionColumnConfigurations?: ConnectionColumnConf[];
  };

  type ConnectionQuery = {
    sinkDataSystemType?: string;
    requisitionState?: string;
    actualState?: string;
    sinkDataCollectionName?: string;
    sinkConnectorId?: number;
    sourceConnectorId?: number;
  };

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
  };

  /**
   -----------------------
   链路详情
   -----------------------
   */
  type KafkaTopic = {
    name?: string;
  };

  type Connector = {
    id?: number;
    name?: string;
    creationTime?: string;
    updateTime?: string;
    desiredState?: string;
    actualState?: string;
  };

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
  };

  type RdbInstance = {
    id?: number;
    host?: string;
    port?: number;
    roleType?: string;
  };

  type ConnectorDetail = {
    id?: number;
    connectionIdWithThisAsSinkConnector?: number;
    connectorType?: string;
    dataSystemType?: string;
    dataSystemClusterName?: string;
    dataSystemResourceName?: string;
    connectorConfigurations?: ConnectorConfiguration[];
  };

  type ConnectorConfiguration = {
    id: number;
    name: string;
    value: string;
  };

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
    source: number; //0: 用户操作, 1: scheduler
    level: number; //0: normal, 1: trace, 2: warning, 3: error, 4: critical
  };
  type EventListQueryparams = {
    current?: number;
    pageSize?: number;
    reason?: string;
    beginTime?: string;
    endTime?: string;
    level?: number;
    source?: number;
    connectorId?: number;
  };

  /**
   -----------------------
   审批相关
   -----------------------
   */
  type ConnectionRequisition = {};

  type ConnectionRequisitionDetail = {
    id?: number;
    sourceApproveResult?: string;
    dbaApproveResult?: string;
    sourceApproverEmail?: string;
    dbaApproverEmail?: string;
    state?: string;
    description?: string;
    connections?: ConnectionDetail[];
  };

  /**
   -----------------------
   1.11 版本模型定义

   data system resource

   connection detail
   -----------------------
   */
  type DataSystemResourceConfiguration = {
    id?: number;
    dataSystemResourceId?: number;
    name?: string;
    value?: string;
    creationTime?: string;
    updateTIme?: string;
  };

  type DataSystemResource = {
    id?: number;
    name?: string;
    resourceType?: string;
    description?: string;
    dataSystemType?: string;
    parentResource?: DataSystemResource;
    projects?: Project[];
    dataSystemResourceConfigurations?: { [key: string]: DataSystemResourceConfiguration };
  };

  type DataSystemResourceQuery = {
    resourceConfigurations?: { [key: string]: any };
    parentResourceId?: number;
    name?: string;
    resourceTypes?: string[];
    projectId?: number;
    current?: number;
    pageSize?: number;
    deleted?: boolean;
    from?: string;
  };

  type DataSystemResourcePageList = {
    data?: DataSystemResource[];
    total?: number;
    success?: boolean;
  };

  type DataFieldDefinition = {
    name?: string;
    type?: string;
    uniqueIndexNames?: string[];
    displayName?: string;
  };

  type DataCollectionDefinition = {
    lowerCaseNameToDataFieldDefinitions?: { [key: string]: DataFieldDefinition };
    uniqueIndexNameToFieldDefinitions?: { [key: string]: DataFieldDefinition[] };
  };

  type ConnectionRequisitionDetail = {
    connections: Connection[];
    description: string;
  };

  type DataSystemResourceDefinition = {
    type: string;
    hasDataCollectionChild: boolean;
    dataCollection: boolean;
    children: { [key: string]: DataSystemResourceDefinition };
  };
}
