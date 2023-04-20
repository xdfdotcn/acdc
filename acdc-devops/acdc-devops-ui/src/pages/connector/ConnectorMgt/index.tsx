import React, {useRef, useState} from 'react';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable, {ActionType} from '@ant-design/pro-table';
import {
  stopConnector, startConnector
} from '@/services/a-cdc/api';
import {message, Drawer, Modal} from 'antd';
import {queryConnectors} from '@/services/a-cdc/api';
import {EditOutlined, PoweroffOutlined, PlaySquareOutlined} from '@ant-design/icons';
import ConnectorDetail from './components/ConnectorDetail';
const {confirm} = Modal;

const ConnectorList: React.FC = () => {
	// 详情抽屉控制
	const [showDetail, setShowDetail] = useState<boolean>(false);

	// 编辑抽屉控制
	const [connectorId, setConnectorId] = useState<number>();

	// 模糊搜索
	const [searchConnectorName, setSearchConnectorName] = useState<string>();

  const ref = useRef<ActionType>();

	/**
	 更新connector 状态
	*/
	const startOrStopConnector = (recordConnectorId: number, isStart: boolean) => {
		confirm({
			title: '确定操作吗?',
			icon: <EditOutlined />,
			content: isStart ? '启动 connector' : '停止 connector',
			async onOk() {
        if (isStart) {
          await startConnector(recordConnectorId)
        } else {
          await stopConnector(recordConnectorId);
        }

        ref.current?.reload(false);
				message.success('操作成功');
			},
			onCancel() {},
		});
	}

	const connectorColumns: ProColumns<API.ConnectorListItem>[] = [
		{
			title: '名称',
			width: "46%",
			dataIndex: 'name',
			render: (dom, entity) => {
				return (
					<a
						onClick={() => {
              setConnectorId(entity.id)
							setShowDetail(true)
						}}
					>
						{entity.name}
					</a>
				);
			},
		},
    {
      title: '预期状态',
      width: "6%",
      dataIndex: 'desiredState',
    },
    {
      title: '实际状态',
      width: "6%",
      dataIndex: 'actualState',
    },
		{
			title: '创建时间',
			width: "15%",
			dataIndex: 'creationTime'
		},
		{
			width: "15%",
			title: '修改时间',
			dataIndex: 'updateTime'
		},
		{
			title: '操作',
			width: "12%",
			valueType: 'option',
			dataIndex: 'option',
			render: (text, record, _, action) => [
				<>
					{
						record.desiredState == 'RUNNING' ?
							<a onClick={() => {startOrStopConnector(record.id!, false)}}>
								<PlaySquareOutlined />
								停止
							</a>
							:
							<a onClick={() => {startOrStopConnector(record.id!, true)}}>
								<PoweroffOutlined />
								启动
							</a>
					}
				</>
			],
		},
	];

	return (
		<div>
			<ProTable<API.ConnectorListItem, API.ConnectorQuery>
				params={{
					name: searchConnectorName
				}}
				rowKey="id"
				// 请求数据API
				request={ queryConnectors }
				columns={connectorColumns}
        actionRef={ref}
				toolbar={{
					search: {
						onSearch: (value: string) => {
							setSearchConnectorName(value);
						},
					},
				}}
				// 分页设置,默认数据,不展示动态调整分页大小
				pagination={{
					showSizeChanger: false,
					pageSize: 10
				}}
				options={false}
				search={false}
			/>

			<Drawer
				width={"100%"}
				visible={showDetail}
				onClose={() => {
          setShowDetail(false)
				}}
				closable={true}
			>
				 <ConnectorDetail connectorId={connectorId} />
			</Drawer>
		</div>
	)
};

export default ConnectorList;
