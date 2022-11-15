/**
 * @see https://umijs.org/zh-CN/plugins/plugin-access
 * */
export default function access(initialState: {currentUser?: API.CurrentUser | undefined}) {
	const {currentUser} = initialState || {};
	// 获取用户失败情况,只保留欢迎页面
	if (!currentUser || !currentUser?.authorities) {
		return {"canAdmin": false}
	}

	const result: API.Authority[] = currentUser?.authorities.filter((val, index, array) => {
		if (val.authority == 'ROLE_ADMIN') {
			return true;
		}
	})

	return {
		canAdmin: currentUser && result && result.length >= 1,
	};
}
