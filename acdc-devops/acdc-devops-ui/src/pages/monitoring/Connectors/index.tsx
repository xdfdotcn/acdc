import {useModel} from '@/.umi/plugin-model/useModel';
import {ConfigKeyConstant} from '@/services/a-cdc/constant/ConfigKeyConstant';
import React from 'react';

const Monitoring: React.FC = () => {
	const {initialState, setInitialState} = useModel('@@initialState');
	const {configs} = initialState;
	const url = configs.get(ConfigKeyConstant.CONFIG_GRAFANA_URL_CONNECTORS)
	return (
		<div style={{border: 0, width: "100%", height: "100%"}}>
			<iframe style={{border: 0, width: "100%", height: "100%"}} src={url}></iframe>
		</div>
	)
};

export default Monitoring;
