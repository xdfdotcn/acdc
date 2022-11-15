import React from 'react';
import ProCard from '@ant-design/pro-card';
import {Descriptions} from 'antd';
import Field from '@ant-design/pro-field';
import ProTable from '@ant-design/pro-table';
import type {ProColumns} from '@ant-design/pro-table';
import {ProFormField} from '@ant-design/pro-form';
import {querySinks} from '@/services/a-cdc/api';
import {useModel} from 'umi';
import FieldMappingList from '@/pages/connector/components/FieldMapping';
import EventList from './eventList';
const SinkJdbcDetail: React.FC = () => {
	const {connectorModel} = useModel('ConnectorModel')

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
						<Field text={connectorModel.sinkConnectorInfo?.srcCluster} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据库类型">
						<Field text={connectorModel.sinkConnectorInfo?.srcDataSystemType} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据库">
						<Field text={connectorModel.sinkConnectorInfo?.srcDatabase} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据表">
						<Field text={connectorModel.sinkConnectorInfo?.srcDataSet} mode="read" />
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
						<Field text={connectorModel.sinkConnectorInfo?.sinkCluster} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据库类型">
						<Field text={connectorModel.sinkConnectorInfo?.sinkDataSystemType} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据库">
						<Field text={connectorModel.sinkConnectorInfo?.sinkDatabase} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据表">
						<Field text={connectorModel.sinkConnectorInfo?.sinkDataSet} mode="read" />
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
				<FieldMappingList />

			</ProCard>
		</>
	);
};

const SinkHiveDetail: React.FC = () => {
	const {connectorModel} = useModel('ConnectorModel')

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
						<Field text={connectorModel.sinkConnectorInfo?.srcCluster} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据库类型">
						<Field text={connectorModel.sinkConnectorInfo?.srcDataSystemType} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据库">
						<Field text={connectorModel.sinkConnectorInfo?.srcDatabase} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据表">
						<Field text={connectorModel.sinkConnectorInfo?.srcDataSet} mode="read" />
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
					<Descriptions.Item label="HIVE 集群">
						<Field text={connectorModel.sinkConnectorInfo?.sinkCluster} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="HIVE 数据库">
						<Field text={connectorModel.sinkConnectorInfo?.sinkDatabase} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="HIVE 数据表">
						<Field text={connectorModel.sinkConnectorInfo?.sinkDataSet} mode="read" />
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
				<FieldMappingList />

			</ProCard>
		</>
	);
};

const SinkKafkaDetail: React.FC = () => {
	const {connectorModel} = useModel('ConnectorModel')

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
						<Field text={connectorModel.sinkConnectorInfo?.srcCluster} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据库类型">
						<Field text={connectorModel.sinkConnectorInfo?.srcDataSystemType} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据库">
						<Field text={connectorModel.sinkConnectorInfo?.srcDatabase} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据表">
						<Field text={connectorModel.sinkConnectorInfo?.srcDataSet} mode="read" />
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
						<Field text={connectorModel.sinkConnectorInfo?.sinkCluster} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="topic">
						<Field text={connectorModel.sinkConnectorInfo?.sinkDataSet} mode="read" />
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
				<FieldMappingList />

			</ProCard>
		</>
	);
};

const SourceRdbDetail: React.FC = () => {
	const {connectorModel}  = useModel('ConnectorModel')
	const sinkJdbcColumns: ProColumns<API.SinkConnectorListItem>[] = [
		{
			title: '名称', width: "24%", dataIndex: 'name',
			render: (dom, entity) => {return (<a > {dom} </a>);},
		},
		{title: '消费主题', width: "18%", dataIndex: 'kafkaTopic'},
		{width: "18%", title: '集群', dataIndex: 'sinkCluster'},
		{title: '数据库', width: "10%", dataIndex: 'sinkDatabase', },
		{title: '数据表', width: "10%", dataIndex: 'sinkDataSet', },
	];
	const sinkHiveColumns: ProColumns<API.SinkConnectorListItem>[] = [
		{
			title: '名称', width: "24%", dataIndex: 'name',
			render: (dom, entity) => {return (<a > {dom} </a>);},
		},
		{title: '消费主题', width: "18%", dataIndex: 'kafkaTopic'},
		{width: "18%", title: '集群', dataIndex: 'sinkCluster'},
		{title: '数据库', width: "10%", dataIndex: 'sinkDatabase', },
		{title: '数据表', width: "10%", dataIndex: 'sinkDataSet', },
	];
	const sinkKafkaColumns: ProColumns<API.SinkConnectorListItem>[] = [
		{
			title: '名称', width: "24%", dataIndex: 'name',
			render: (dom, entity) => {return (<a > {dom} </a>);},
		},
		{width: "18%", title: '集群', dataIndex: 'sinkDataSet'},
		{title: '消费主题', width: "18%", dataIndex: 'kafkaTopic'},
		{title: '存储主题', width: "10%", dataIndex: 'sinkDataSet', },
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
						<Field text={connectorModel.sourceConnectorInfo?.srcCluster} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据库类型">
						<Field text={connectorModel.sourceConnectorInfo?.srcDataSystemType} mode="read" />
					</Descriptions.Item>
					<Descriptions.Item label="数据库">
						<Field text={connectorModel.sourceConnectorInfo?.srcDatabase} mode="read" />
					</Descriptions.Item>
					{
						connectorModel!.sourceConnectorInfo!.srcDataSystemType == 'tidb' ?
							<Descriptions.Item label="消费主题">
								<Field text={connectorModel.sourceConnectorInfo?.kafkaTopic} mode="read" />
							</Descriptions.Item>
							: <></>
					}
				</Descriptions>
			</ProCard>
			<ProCard
				title="目标端信息"
				headerBordered
				onCollapse={(collapse) => {}}
				tabs={{
					type: 'card',
					//onChange:(key)=>{message.info(key)}
				}}

			>
					<ProCard.TabPane key="tab1" tab="JDBC">
						<ProTable<API.SinkConnectorListItem, API.SinkConnectorQuery>
							params={{
								sourceConnectorId: connectorModel.sourceConnectorInfo?.connectorId,
								sinkDataSystemType:'mysql'
							}}
							request={querySinks}
							columns={sinkJdbcColumns}
							pagination={{
								showSizeChanger: false,
								pageSize: 8
							}}
							options={false}
							rowKey="id"
							search={false}
						/>

					</ProCard.TabPane>

					<ProCard.TabPane key="tab2" tab="HIVE">
						<ProTable<API.SinkConnectorListItem, API.SinkConnectorQuery>
							params={{
								sourceConnectorId: connectorModel.sourceConnectorInfo?.connectorId,
								sinkDataSystemType:'hive'
							}}
							request={querySinks}
							columns={sinkHiveColumns}
							pagination={{
								showSizeChanger: false,
								pageSize: 8
							}}
							options={false}
							rowKey="id"
							search={false}
						/>


					</ProCard.TabPane>

					<ProCard.TabPane key="tab3" tab="KAFKA">

						<ProTable<API.SinkConnectorListItem, API.SinkConnectorQuery>
							params={{
								sourceConnectorId: connectorModel.sourceConnectorInfo?.connectorId,
								sinkDataSystemType: 'kafka'
							}}
							request={querySinks}
							columns={sinkKafkaColumns}
							pagination={{
								showSizeChanger: false,
								pageSize: 8
							}}
							options={false}
							rowKey="id"
							search={false}
						/>

					</ProCard.TabPane>
			</ProCard>

		</>
	);
};


const ConnectorDetail: React.FC = () => {
	const {connectorModel, setConnectorModel} = useModel('ConnectorModel')
	const getSinkLinkPage = () => {

		let conenctorType = connectorModel.connectorType

		// sink
		if (conenctorType == 'SINK') {
			if (connectorModel!.sinkConnectorInfo!.sinkDataSystemType == 'mysql') {
				return <SinkJdbcDetail />

			}

			if (connectorModel!.sinkConnectorInfo!.sinkDataSystemType == 'tidb') {
				return <SinkJdbcDetail />

			}

			if (connectorModel!.sinkConnectorInfo!.sinkDataSystemType == 'hive') {
				return <SinkHiveDetail />

			}
			if (connectorModel!.sinkConnectorInfo!.sinkDataSystemType == 'kafka') {
				return <SinkKafkaDetail />
			}
		}

		// source
		if (conenctorType == 'SOURCE') {
			return <SourceRdbDetail />
		}
	}


	return (
		<ProCard
			tabs={{
				type: 'card',
				//onChange:(key)=>{message.info(key)}
			}}
		>
			<ProCard.TabPane key="tab1" tab="链路详情">
				{getSinkLinkPage()}
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
						text={JSON.stringify(connectorModel.connectorConfig)}
					/>
				</ProCard>

			</ProCard.TabPane>
			{
				connectorModel.connectorId?
				<ProCard.TabPane key="tab3" tab="事件列表">
					<EventList connectorId = {connectorModel.connectorId}/>
				</ProCard.TabPane> : null
			}

		</ProCard>
	);
};

export default ConnectorDetail;
