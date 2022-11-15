import React, {useState} from 'react';
import {useModel} from 'umi';
import {ProFormText} from '@ant-design/pro-form';

type Pprops = {
	onHostChange: (host?: string) => void;
	onPortChange: (port?: string) => void;
}
const RdbMysqlEdit: React.FC<Pprops> = (props) => {
	// 全局数据流
	const {rdbMysqlModel,setRdbMysqlModel} = useModel('RdbMysqlModel')

	const {onHostChange,onPortChange} = props;

	return (
		<div>
			<ProFormText name="host" fieldKey='1' label="实例地址"
			disabled={rdbMysqlModel.source=='detail'}
			value={rdbMysqlModel?.host}
				onChange={
					(dom) => {
						onHostChange(dom.target.value,'')
					}
			}/>
			<ProFormText name="port" fieldKey='2' label="实例端口" 
			value={rdbMysqlModel?.port}
			disabled={rdbMysqlModel.source=='detail'}
				onChange={
					(dom) => {
						onPortChange(dom.target.value)
					}
				} />
		</div>
	);
};

export default RdbMysqlEdit;


