// https://umijs.org/config/
import {defineConfig} from 'umi';

export default defineConfig({
	define: {
		GRAFANA_URL: 'GRAFANA_URL'
	},
	plugins: [
		// https://github.com/zthxxx/react-dev-inspector
		'react-dev-inspector/plugins/umi/react-inspector',
	],
	// https://github.com/zthxxx/react-dev-inspector#inspector-loader-props
	inspectorConfig: {
		exclude: [],
		babelPlugins: [],
		babelOptions: {},
	},
});
