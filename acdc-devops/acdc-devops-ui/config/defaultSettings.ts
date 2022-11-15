import {Settings as LayoutSettings} from '@ant-design/pro-layout';

const Settings: LayoutSettings & {
	pwa?: boolean;
	logo?: string;
} = {
	navTheme: 'light',
	// 拂晓蓝
	primaryColor: '#008B8B',
	layout: 'mix',
	contentWidth: 'Fluid',
	fixedHeader: false,
	fixSiderbar: true,
	colorWeak: false,
	title: '',
	pwa: false,
	logo: '/logo.png',
	iconfontUrl: '',
};

export default Settings;
