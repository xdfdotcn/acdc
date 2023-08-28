import React, {useEffect, useState} from 'react';
import ProCard from '@ant-design/pro-card';
import {Descriptions} from 'antd';
import Field from '@ant-design/pro-field';
import ProTable from '@ant-design/pro-table';
import type {ProColumns} from '@ant-design/pro-table';
import {ProFormField} from '@ant-design/pro-form';
import {getConnectionDetail, getConnectorDetail, queryConnection} from '@/services/a-cdc/api';
import FieldMappingList from '@/pages/connector/components/FieldMapping';
import EventList from './eventList';
import ConnectionColumnConf from '@/pages/connection/components/ConnectionColumnConf';

const SinkDetail: React.FC<{connectorDetail: API.ConnectorDetail}> = ({connectorDetail}) => {

  const [connectionDetail, setConnectionDetail] = useState<API.ConnectionDetail>({});

  const doGetConnectionDetail = async () => {
    const connectionQuery: API.ConnectionQuery = {
      sinkConnectorId: connectorDetail.id
    }
    const queriedConnections = await queryConnection({...connectionQuery});
    const resultConnectionDetail = await getConnectionDetail(queriedConnections.data[0].id)
    setConnectionDetail(resultConnectionDetail)
  }

  useEffect(() => {
    doGetConnectionDetail();
  }, [connectorDetail.id])

	return (
		<>
			<ProCard
				title="源端信息"
				headerBordered
				collapsible
				onCollapse={(collapse) => {return true}}
			>
				<Descriptions column={2}>
					<Descriptions.Item label="集群">
						<Field text={connectionDetail?.sourceDataSystemClusterName} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据库类型">
						<Field text={connectionDetail?.sourceDataSystemType} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据库">
						<Field text={connectionDetail?.sourceDatabaseName} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据表">
						<Field text={connectionDetail?.sourceDataCollectionName} mode="read" />
					</Descriptions.Item>
				</Descriptions>
			</ProCard>
			<ProCard
				title="目标端信息"
				headerBordered
				collapsible
				onCollapse={(collapse) => {}}
			>
				<Descriptions column={2}>
					<Descriptions.Item label="集群">
						<Field text={connectionDetail?.sinkDataSystemClusterName} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据库类型">
						<Field text={connectionDetail?.sinkDataSystemType} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据库">
						<Field text={connectionDetail?.sinkDatabaseName} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据表">
						<Field text={connectionDetail?.sinkDataCollectionName} mode="read" />
					</Descriptions.Item>
				</Descriptions>

			</ProCard>
			<ProCard
				title="字段映射"
				headerBordered
				collapsible
				defaultCollapsed
				onCollapse={(collapse) => {}}
			>
        <ConnectionColumnConf
          columnConfProps={{
            displayDataSource: connectionDetail.connectionColumnConfigurations,
            originalDataSource: connectionDetail.connectionColumnConfigurations!,
            sinkDataSystemType: connectionDetail.sinkDataSystemType,
            canEdit: false,
            canDelete: false,
          }}
        />
			</ProCard>
		</>
	);
};

const SourceDetail: React.FC<{connectorDetail: API.ConnectorDetail}> = ({connectorDetail}) => {
  const [ticdcTopicName, setTicdcTopicName] = useState<string>("");

  useEffect(() => {
    if (connectorDetail.dataSystemType == "TIDB") {
      for (let i = 0; i < connectorDetail.connectorConfigurations?.length; i++) {
        if (connectorDetail.connectorConfigurations[i].name == "source.kafka.topic") {
          setTicdcTopicName(connectorDetail.connectorConfigurations[i].value)
          break;
        }
      }
    }
  }, [connectorDetail.id])

	const columns: ProColumns<API.Connection>[] = [
		{
			title: '名称', width: "24%", dataIndex: 'sinkConnectorName',
			render: (dom, entity) => {return (<a > {dom} </a>);},
		},
    {title: '源数据集', width: "10%", dataIndex: 'sourceDataCollectionName'},
    {title: '数据数据系统类型', width: "18%", dataIndex: 'sinkDataSystemType'},
    {title: '数据目标项目', width: "18%", dataIndex: 'sinkProjectName'},
		{title: '数据目标路径', width: "18%", dataIndex: 'sinkDataCollectionPath'},
		{title: '目标数据集', width: "18%", dataIndex: 'sinkDataCollectionName'},
	];

	return (
		<>
			<ProCard
				title="源端信息"
				headerBordered
				collapsible
				onCollapse={(collapse) => {}}
			>
				<Descriptions column={2}>
					<Descriptions.Item label="集群">
						<Field text={connectorDetail.dataSystemClusterName} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据库类型">
						<Field text={connectorDetail.dataSystemType} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="源数据资源名称（database/topic）">
						<Field text={connectorDetail.dataSystemResourceName} mode="read" />
					</Descriptions.Item>
					{
            connectorDetail.dataSystemType == 'TIDB' ?
							<Descriptions.Item label="消费主题">
								<Field text={ticdcTopicName} mode="read" />
							</Descriptions.Item>
							: <></>
					}
				</Descriptions>
			</ProCard>
			<ProCard
				title="目标端信息"
				headerBordered
				onCollapse={(collapse) => {}}
			>
        <ProTable<API.Connection, API.ConnectionQuery>
          params={{
            sourceConnectorId: connectorDetail.id
          }}
          request={queryConnection}
          columns={columns}
          pagination={{
            showSizeChanger: false,
            pageSize: 10
          }}
          options={false}
          rowKey="id"
          search={false}
        />
			</ProCard>

		</>
	);
};


const ConnectorDetail: React.FC<{connectorId: number}> = ({connectorId}) => {

  const [connectorDetail, setConnectorDetail] = useState<API.ConnectorDetail>({})

  const [connectorConfiguration, setConnectorConfiguration] = useState<string>("")

  const showConnectorDetail = async () => {
    const connector = await getConnectorDetail(connectorId)
    if (!connector) {
      return
    }
    setConnectorDetail(connector)

    let configurationStr = "{\n"
    connector.connectorConfigurations?.forEach((element) => {
      configurationStr += "  " + "\""+ element.name +"\""+ ": " +"\""+ element.value+"\"" + ",\n"
    })
    configurationStr = configurationStr.slice(0, configurationStr.length - 2)
    configurationStr += "\n}"
    setConnectorConfiguration(configurationStr)
  }

  const getConnectorDetailPage = () => {
    if (!connectorDetail||!connectorDetail.id) {
      return <></>
    }
    if (connectorDetail.connectorType == 'SINK') {
      return <SinkDetail connectorDetail={connectorDetail} />
    }
    return <SourceDetail connectorDetail={connectorDetail} />
  }

  useEffect(() => {
    showConnectorDetail();
  },[connectorId]);

	return (
		<ProCard
			tabs={{
				type: 'card',
			}}
		>
			<ProCard.TabPane key="tab1" tab="链路详情">
        {getConnectorDetailPage()}
			</ProCard.TabPane>

			<ProCard.TabPane key="tab2" tab="链路配置">
				<ProCard title="配置信息" headerBordered collapsible>
					<ProFormField
						ignoreFormItem
						fieldProps={{
							style: {
								width: '100%',
								background: 'black'
							},
						}}
						mode="read"
						valueType="jsonCode"
						text={connectorConfiguration}
					/>
				</ProCard>

			</ProCard.TabPane>
			{
				<ProCard.TabPane key="tab3" tab="事件列表">
					<EventList connectorId = {connectorId}/>
				</ProCard.TabPane>
			}

		</ProCard>
	);
};

export default ConnectorDetail;
