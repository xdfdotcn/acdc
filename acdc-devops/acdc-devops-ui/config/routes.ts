export default [
	{
		path: '/',
		redirect: '/welcome',
	},

	{
		path: '/monitoring/connectors',
		name: 'connector-monitoring',
		icon: 'smile',
		access: 'canAdmin',
		// 文件夹小写也是没有毛病的
		component: './monitoring/Connectors',
	},

	{
		path: '/connector/connectorMgt',
		name: 'connectorMgt',
		icon: 'smile',
		access: 'canAdmin',
		// 文件夹小写也是没有毛病的
		component: './connector/ConnectorMgt',
	},

	{
		path: '/connector/newConnectorApply',
		name: 'connectorApplyNew',
		icon: 'smile',
		component: './connector/ConnectionApply',
	},

	{
		path: '/connection/ConnectionMgt',
		name: 'connectionMgt',
		icon: 'smile',
		component: './connection/ConnectionMgt',
	},

	// 审批操作页面
	{
		path: '/connection/connectionRequisition/:connectionRequisitionId/approval',
		component: './connection/connectionRequisition/Approval',
	},

	{
		path: '/user/login',
		component: './user/Login',
	},


	{
		path: '/project-mgt',
		name: 'project-mgt',
		icon: 'smile',
		access: 'canAdmin',
		// 文件夹小写也是没有毛病的
		component: './ProjectMgt',
	},

	// 用户手册
	{
		path: '/doc/quickstart',
		name: 'doc.quick-start',
		icon: 'smile',
		component: './doc/Quickstart',
	},

	{
		path: '/welcome',
		name: 'welcome',
		icon: 'smile',
		component: './Welcome',
	},

	{
		component: './404',
	},
];
