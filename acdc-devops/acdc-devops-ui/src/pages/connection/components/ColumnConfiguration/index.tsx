import React from 'react';
import ProCard from "@ant-design/pro-card";
import {ProColumns, ProTable} from "@ant-design/pro-table";
import {Space} from "antd";

const ColumnConfiguration: React.FC<{
	connectionColumnConfigurations: API.FieldMappingListItem[]
	, from?: string
}> = ({connectionColumnConfigurations, from}) => {
	let configurationsToShow: API.FieldMappingListItem[];

	if (connectionColumnConfigurations
		&& connectionColumnConfigurations.length > 0
	) {

		if ('info' != from) {
			configurationsToShow = connectionColumnConfigurations
				.filter(connectionColumnConfiguration => connectionColumnConfiguration.sourceFieldFormat != "")
		} else {
			configurationsToShow = connectionColumnConfigurations

		}
	}
	const columns: ProColumns<API.FieldMappingListItem>[] = [
		{
			dataIndex: 'sinkFieldFormat',
			title: '目标字段名',
			render: (_, fieldMappingListItem) => (
				<span>
					{fieldMappingListItem.sinkFieldFormat}
				</span>
			),
		},

		{
			dataIndex: 'sourceFieldFormat',
			title: '源字段名',
			render: (_, fieldMappingListItem) => (
				<span>
					{fieldMappingListItem.sourceFieldFormat}
				</span>
			),
		},
		{
			dataIndex: 'filterExpress',
			title: '过滤规则',
			render: (_, fieldMappingListItem) => (
				<span>
					{fieldMappingListItem.filterOperator + " " + fieldMappingListItem.filterValue}
				</span>
			),
		},
	];

  return (
    <ProTable<API.FieldMappingListItem>
      columns={columns}
			dataSource={configurationsToShow}
      rowKey="id"
      pagination={false}
      toolBarRender={false}
      search={false}
    />
  );
};

export default ColumnConfiguration;
