import React from 'react';
import {useModel} from 'umi';
import {ProFormText} from '@ant-design/pro-form';

const RdbTidbEdit: React.FC = () => {
	// 全局数据流
	const {rdbTidbModel, setRdbTidbModel} = useModel('RdbTidbModel')
	return (
		<>
			<ProFormText name="topic" label="订阅主题"
				disabled={rdbTidbModel.source == 'detail'}
				value={rdbTidbModel?.topic}
				onChange={
					(dom) => {
						setRdbTidbModel({
							...rdbTidbModel,
							topic: dom.target.value
						})
					}
				} />
		</>
	);
};

export default RdbTidbEdit;

