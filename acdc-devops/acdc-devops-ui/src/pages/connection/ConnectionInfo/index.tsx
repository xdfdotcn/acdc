import React, {useState} from 'react';
import ProCard from '@ant-design/pro-card';
import ConnectionDetail from '../components/Detail';
import ProTable, {ProColumns} from '@ant-design/pro-table';
import {queryConnectonRequisition, querySinks} from '@/services/a-cdc/api';
import { format } from 'react-string-format';
import { useAccess, Access, useModel } from 'umi';
import {ConfigKeyConstant} from '@/services/a-cdc/constant/ConfigKeyConstant';
const ConnectionInfo: React.FC<{connectionDetail: API.ConnectionDetail}> = ({connectionDetail}) => {
  const access = useAccess();
  const { initialState, setInitialState } = useModel('@@initialState');
	const {configs} = initialState;
  const grafanaParameter = format('&var-source_connector_name={0}&var-sink_connector_name={1}&kiosk=tv', connectionDetail.sourceConnectorName, connectionDetail.sinkConnectorName);
  const monitorMysqlSource = configs.get(ConfigKeyConstant.CONFIG_GRAFANA_URL_MYSQLSOURCECONNECTOR) + grafanaParameter
	const monitorTidbSource = configs.get(ConfigKeyConstant.CONFIG_GRAFANA_URL_TIDBSOURCECONNECTOR) + grafanaParameter
	const monitorConnectionForUser=configs.get(ConfigKeyConstant.CONFIG_GRAFANA_URL_CONNECTIONFORUSER) + grafanaParameter
	const monitorConnectionForOperation=configs.get(ConfigKeyConstant.CONFIG_GRAFANA_URL_CONNECTIONFOROPERATOR) + grafanaParameter

  const getSourceConnectorMonitorPage = () => {
    switch (connectionDetail.sourceDataSystemType) {
      case "MYSQL": return (
        <ProCard.TabPane key="monitor_mysql_source_connector" tab="监控页面(MySQL source connector)" disabled={!access.canAdmin}>
          <iframe style={{border: 0, width: "100%", height: "85vh"}} src={monitorMysqlSource}></iframe>
        </ProCard.TabPane>
      )
        break;
      case "TIDB": return (
        <ProCard.TabPane key="monitor_tidb_source_connector" tab="监控页面(tidb source connector)" disabled={!access.canAdmin}>
          <iframe style={{border: 0, width: "100%", height: "85vh"}} src={monitorTidbSource}></iframe>
        </ProCard.TabPane>
      )
        break;
      default: return <></>;
    }
  }

  const connectionRequisitionColumns: ProColumns<API.ConnectionRequisition>[] = [
		{title: '描述', width: "9%", dataIndex: 'description'},
		{title: '审批状态', width: "9%", dataIndex: 'state'},
		{title: '源端审批人', width: "9%", dataIndex: 'sourceApproverEmail', },
		{title: '源端审批结果', width: "15%", dataIndex: 'sourceApproveResult', },
		{title: 'DBA审批人', width: "15%", dataIndex: 'dbaApproverEmail', },
		{title: 'DBA审批结果', width: "15%", dataIndex: 'dbaApproveResult', },
		{title: '申请时间', width: "15%", dataIndex: 'creationTimeFormat', },
	];

	return (
		<ProCard
			tabs={{
				type: 'card',
				//onChange:(key)=>{message.info(key)}
			}}
		>
			<ProCard.TabPane key="tab1" tab="链路详情">
				<ConnectionDetail index={connectionDetail.id} connectionDetail={connectionDetail} from='info' />
			</ProCard.TabPane>

			<ProCard.TabPane key="tab2" tab="审批记录">
				<ProTable<API.ConnectionRequisition>
					params={{
						connectionId: connectionDetail.id,
					}}
					request={queryConnectonRequisition}
					columns={connectionRequisitionColumns}
					pagination={{
						showSizeChanger: false,
						pageSize: 8
					}}
					options={false}
					rowKey="id"
					search={false}
				/>
			</ProCard.TabPane>

			<ProCard.TabPane key="monitor_connection_for_user" tab="监控页面">
				<iframe style={{border: 0, width: "100%", height: "85vh"}} src={monitorConnectionForUser}></iframe>
			</ProCard.TabPane>

      <ProCard.TabPane key="monitor_connection_for_operation" tab="监控页面(运维)" disabled={!access.canAdmin}>
        <iframe style={{border: 0, width: "100%", height: "85vh"}} src={monitorConnectionForOperation}></iframe>
      </ProCard.TabPane>

      {
        getSourceConnectorMonitorPage()
      }
		</ProCard>
	);
};

export default ConnectionInfo;
