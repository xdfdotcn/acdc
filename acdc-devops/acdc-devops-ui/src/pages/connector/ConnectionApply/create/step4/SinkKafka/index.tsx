import React, {useEffect, useState} from 'react';
import ProCard from '@ant-design/pro-card';
import RcResizeObserver from 'rc-resize-observer';
import type {ProColumns} from '@ant-design/pro-table';
import ProTable from '@ant-design/pro-table';
import styles from './split.less';
import {queryKafkaTopics} from '@/services/a-cdc/api';
import {useModel} from 'umi';
import { Input } from 'antd';

const { Search } = Input;

const TopicList: React.FC = () => {
	const topicColumns: ProColumns<API.KafkaTopic>[] = [
		{
			title: 'topic',
			dataIndex: 'name',
		}
	];

	const {applyInfoModel, setApplyInfoModel} = useModel('ConnectorApplyModel')
	// 选中
	const [selectId, setSelectId] = useState<number>();
	// 模糊查询
	const [qTopic, setQTopic] = useState<string>();

	const onSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
		const { value } = e.target;
		setQTopic(value);
	};
	const onSearch = (value: string) => {
		setQTopic(value);
	};

	useEffect(()=>{
		if(applyInfoModel.sinkDataSetId && applyInfoModel.sinkDataSetName) {
			if(applyInfoModel.sinkSearchDataSet) {
				setQTopic(applyInfoModel.sinkSearchDataSet);
			}
			setSelectId(applyInfoModel.sinkDataSetId);
		}
	},[applyInfoModel]);

	return (
		<ProTable<API.KafkaTopic>
			onRow={(record) => {
				return {
					onClick: () => {
						setSelectId(record.id);
						setApplyInfoModel({
							...applyInfoModel,
							sinkDataSetId: record.id,
							sinkDataSetName: record.name,
							sinkSearchDataSet: qTopic
						})
					},
				};
			}}
			rowClassName={(record) => {
				return record.id === selectId ? styles['split-row-select-active'] : '';
			}}
			params={{
				kafkaClusterId: applyInfoModel.sinkClusterId,
				name: qTopic,
			}}
			request={queryKafkaTopics}
			columns={topicColumns}
			toolbar={{
				search: <Search defaultValue={qTopic} value={qTopic} onChange={onSearchChange} onSearch={onSearch}/>
			}}
			options={false}
			rowKey={(record) => String(record.id)}
			search={false}
			pagination={{
				showSizeChanger: false,
				pageSize: 10
			}}
		/>
	)
};


const ConvertList: React.FC = () => {
	const convertListdataSource: API.KafkaConvertListItem[] = [
		//{id: 1, name: 'CDCV1', convert: 'CDC_V1'},
		{id: 2, name: 'JSON', convert: 'JSON'},
		{id: 3, name: 'SCHEMA_LESS_JSON', convert: 'SCHEMA_LESS_JSON'}
	];
	const convertColumns: ProColumns<API.KafkaConvertListItem>[] = [
		{
			title: '序列化方式',
			dataIndex: 'name',
		}
	];

	const {applyInfoModel, setApplyInfoModel} = useModel('ConnectorApplyModel')

	// 选中
	const [selectConvert, setSelectConvert] = useState<string>();

	useEffect(()=>{
		if(applyInfoModel.sinkKafkaConverterType) {
			setSelectConvert(applyInfoModel.sinkKafkaConverterType);
		}
	},[applyInfoModel]);
	return (
		<ProTable<API.KafkaConvertListItem>
			onRow={(record) => {
				return {
					onClick: () => {
						setSelectConvert(record.convert);
						setApplyInfoModel({
							...applyInfoModel,
							sinkKafkaConverterType: record.convert,
							specificConfiguration: record.convert
						})
					},
				};
			}}
			rowClassName={(record) => {
				if (record.convert === selectConvert
					&& applyInfoModel!.sinkKafkaConverterType! === selectConvert) {
					return styles['split-row-select-active']
				}
				return ''
			}}
			dataSource={convertListdataSource}
			columns={convertColumns}
			options={false}
			rowKey={(record) => String(record.id)}
			search={false}
			pagination={{
				showSizeChanger: false,
				pageSize: 10
			}}
		/>
	)
};

// 主页面

const MainPage: React.FC = () => {
	const [responsive, setResponsive] = useState(false);
	return (
		<ProCard
			bordered
			hoverable
			headerBordered
			style={{
				marginBottom: 16,
				minWidth: 800,
				maxWidth: '100%',
			}}>

			<RcResizeObserver
				key="resize-observer"
				onResize={(offset) => {
					setResponsive(offset.width < 100);
				}}
			>
				<ProCard bordered split={responsive ? 'horizontal' : 'vertical'} >
					<ProCard colSpan="50%">
						<TopicList />
					</ProCard>
					<ProCard colSpan="50%" >
						<ConvertList />
					</ProCard>
				</ProCard>
			</RcResizeObserver>
		</ProCard>
	)
};

export default MainPage;
