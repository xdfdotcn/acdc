export default [
  {
    path: '/',
    redirect: '/welcome',
  },

  /**
  =================================
  监控大盘
  =================================
  */
  {
    path: '/monitoring/connectors',
    name: 'connector-monitoring',
    icon: 'smile',
    access: 'canAdmin',
    // TODO 组建
    component: './monitoring/Connectors',
  },

  /**
  =================================
  任务管理
  =================================
  */
  {
    path: '/connector/connector-mgt',
    name: 'connector-mgt',
    icon: 'smile',
    access: 'canAdmin',
    component: './connector/ConnectorMgt',
  },

  /**
  =================================
  链路申请
  =================================
  */
  {
    path: '/connction-requisition',
    name: 'connction-requisition',
    icon: 'smile',
    component: './ConnectionRequisition',
  },

  /**
  =================================
  链路管理
  =================================
  */
  {
    path: '/connection/connection-mgt',
    name: 'connection-mgt',
    icon: 'smile',
    component: './connection/ConnectionMgt',
  },

  /**
   =================================
   宽表申请
   =================================
   */
  {
    path: '/wide-table/requisition',
    name: 'wide-table.requisition',
    icon: 'smile',
    access: 'canAdmin',
    component: './WideTableRequisition',
  },
  /**
   =================================
   宽表管理
   =================================
   */
  {
    path: '/wide-table/mgt',
    name: 'wide-table.mgt',
    icon: 'smile',
    access: 'canAdmin',
    component: './WideTable/WideTableMgt',
  },

  /**
  =================================
  审批
  =================================
  */
  {
    path: '/connection/connectionRequisition/:connectionRequisitionId/approval',
    component: './connection/connectionRequisition/Approval',
  },

  /**
  =================================
  登录
  =================================
  */
  {
    path: '/user/login',
    component: './user/Login',
  },

  /**
  =================================
  项目管理
  =================================
  */
  {
    path: '/project-mgt',
    name: 'project-mgt',
    icon: 'smile',
    access: 'canAdmin',
    component: './ProjectMgt',
  },

  /**
  =================================
  快速开始
  =================================
  */
  {
    path: '/doc/quickstart',
    name: 'doc.quick-start',
    icon: 'smile',
    component: './doc/Quickstart',
  },

  /**
  =================================
  欢迎页
  =================================
  */
  {
    path: '/welcome',
    name: 'welcome',
    icon: 'smile',
    component: './Welcome',
  },

  /**
  =================================
  404
  =================================
  */
  {
    component: './404',
  },
];
