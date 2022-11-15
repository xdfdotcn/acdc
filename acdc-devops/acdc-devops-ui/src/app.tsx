import type {Settings as LayoutSettings} from '@ant-design/pro-layout';
import {PageLoading} from '@ant-design/pro-layout';
import type {RunTimeLayoutConfig} from 'umi';
import {history} from 'umi';
import RightContent from '@/components/RightContent';
import Footer from '@/components/Footer';
import {getUiConfig, login as getLoginGuider} from './services/a-cdc/api';
import type {RequestConfig} from 'umi'; //引入request配置问题类型 通过类型去推测怎么使用
import {message} from 'antd';
import {RoutesConstant} from './services/a-cdc/constant/RoutesConstant';
import {UserConstant} from './services/a-cdc/constant/UserConstant';
const isDev = process.env.NODE_ENV === 'development';

/** 获取用户信息比较慢的时候会展示一个 loading */
export const initialStateConfig = {
	loading: <PageLoading />,
};

/**
 * @see  https://umijs.org/zh-CN/plugins/plugin-initial-state
 * */
export async function getInitialState(): Promise<{
	settings?: Partial<LayoutSettings>;
	currentUser?: API.CurrentUser;
	fetchUserInfo?: () => Promise<API.CurrentUser | undefined>;
	configs?:Map<String,String>;
	fetchConfig?: () => Promise<Map<String, String | undefined>>;
}> {
	const fetchUserInfo = async () => {
		try {
			let loginParams: API.LoginParams = {
				loginSuccessUrl: history.location.pathname,
				loginUrl: RoutesConstant.LOGIN_PATH
			}
			const guider = await getLoginGuider(loginParams);
			return guider.user!;
		} catch (error) {
			//history.push(loginPath);
			message.error("登录异常")
		}

		return undefined;
	};

	const fetchConfig = async () => {
		try {
			const result = await getUiConfig()
			const config= new Map(Object.entries(result))
			return config;
		}
		catch (error) {
			message.error("获取配置失败")
			return new Map();
		}
	}

	// 获取 UI 配置信息
	const configs = await fetchConfig();

	// 非E2登录,登录成功后会把用户信息放入到 init state 中
	let historyPath = history.location.pathname;
	let fullLoginPath = RoutesConstant.LOGIN_PATH + "/"
	let loginPath = RoutesConstant.LOGIN_PATH
	if (historyPath == fullLoginPath
		|| historyPath == loginPath
	) {
		return {
			fetchUserInfo,
			settings: {},
			configs:configs,
		};
	}
		// 登录页面,只提供获取用户的信息的方法
		const currentUser = await fetchUserInfo();
		return {
			fetchUserInfo: fetchUserInfo,
			currentUser: currentUser,
			settings: {},
			configs:configs
		};
}

// ProLayout 支持的api https://procomponents.ant.design/components/layout
export const layout: RunTimeLayoutConfig = ({initialState}) => {
	return {
		rightContentRender: () => <RightContent />,
		disableContentMargin: false,
		waterMarkProps: {
			content: initialState?.currentUser?.username,
		},
		footerRender: () => <Footer />,
		//links: isDev
		//? [
		//<Link to="/umi/plugin/openapi" target="_blank">
		//<LinkOutlined />
		//<span>OpenAPI 文档</span>
		//</Link>,
		//<Link to="/~docs">
		//<BookOutlined />
		//<span>业务组件文档</span>
		//</Link>,
		//]
		//: [],
		menuHeaderRender: undefined,
		// 自定义 403 页面
		// unAccessible: <div>unAccessible</div>,
		...initialState?.settings,
	};
};


/** 请求响应拦截器,通用处理权限,500,404 等非200的响应
参考链接:
https://blog.csdn.net/qq_27175079/article/details/111996729
https://pro.ant.design/zh-CN/docs/request/
https://umijs.org/zh-CN/plugins/plugin-request#responseinterceptors
https://www.ujcms.com/knowledge/469.html

*/

async function responseInterceptors(response: Response) {
	try {
		const resJson = await response.clone().json();
		// 登录/登出跳转, 错误异常拦截器,只拦截4xxxx, 其他非200视为502错误
		if (response && response.status == 401) {
			window.location.href = resJson.url
		}
		return resJson;
	} catch (e) {
		return {};
	}
}

// 请求拦截器
const authHeaderInterceptor = (url: string, options: RequestConfig) => {
	let savedToken = sessionStorage.getItem(UserConstant.USER_TOKEN_KEY)
	let authToken = savedToken === null ? "" : savedToken
	let loginUrl = window.location.protocol + "//" + window.location.host + RoutesConstant.LOGIN_PATH
	let loginSuccessUrl = history.location.pathname
	const authHeader = {
		Authorization: 'Bearer ' + authToken,
		loginUrl: loginUrl,
		loginSuccessUrl: loginSuccessUrl
	};
	return {
		url: `${url}`,
		options: {...options, interceptors: true, headers: authHeader},
	};
};


export const request: RequestConfig = {
	//errorHandler:(errorMsg)=>{window.location.href='https://www.baidu.com/'},
	requestInterceptors: [authHeaderInterceptor],
	responseInterceptors: [responseInterceptors],
};

