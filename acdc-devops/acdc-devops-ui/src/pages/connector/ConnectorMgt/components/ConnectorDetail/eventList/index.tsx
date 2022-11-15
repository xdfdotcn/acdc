import React, {useState} from 'react';
import ProTable from '@ant-design/pro-table';
import { getEventList } from '@/services/a-cdc/api';
import type {ProColumns} from '@ant-design/pro-table';
import { Tag } from 'antd';

interface EventListProps {
  connectorId: number;
}

const SourceObj = {
  0: '用户操作', 
  1: 'acdc scheduler',
};
const LevelObj = {
  0: 'trace', 
  1: 'debug',
  2: 'info',
  3: 'warn',
  4: 'error',
  5: 'fatal'
};
const colorObj = {
  'info': 'green', 
  'trace': 'blue', 
  'warn': 'yellow', 
  'error': 'red', 
  'fatal': 'white', 
  'debug': 'purple',
}
const eventColumns: ProColumns<API.ConnectorMgtEventItem>[] = [
  {
    title: 'ID',
    dataIndex: 'id',
    search: false,
  },
  {
    title: '来源',
    dataIndex: 'source',
    render: (_, record) => SourceObj[record.source]
  },
  {
    title: '等级',
    dataIndex: 'level',
    render: (_, record) => <Tag color={colorObj[LevelObj[record.level]]}>{LevelObj[record.level]}</Tag>
  },
  {
    title: '事件来由',
    dataIndex: 'reason'
  },
  {
    title: '创建时间',
    dataIndex: 'creationTime',
  },
  {
    title: '修改时间',
    dataIndex: 'updateTime',
  },
  {
    title: '说明',
    dataIndex: 'message'
  },
];


const EventList: React.FC<EventListProps> = (props) => {
  const { connectorId } = props;
  return (
  <ProTable<API.ConnectorMgtEventItem, API.EventListQueryparams>
    params={{
      connectorId:connectorId,
    }}
    request={(params,option) => getEventList(connectorId, params, option)}
    columns={eventColumns}
    rowKey="id"
  />);
};

export default EventList;
