import {
  LockOutlined,
  UserOutlined,
} from '@ant-design/icons';
import { message, Tabs } from 'antd';
import React, { useState } from 'react';
import { ProFormText, LoginForm } from '@ant-design/pro-form';
import { useIntl, history, FormattedMessage, SelectLang, useModel } from 'umi';
import { login } from '@/services/a-cdc/api';

import styles from './index.less';
import {UserConstant} from '@/services/a-cdc/constant/UserConstant';
import {RoutesConstant} from '@/services/a-cdc/constant/RoutesConstant';

const Login: React.FC = () => {
  const { initialState, setInitialState } = useModel('@@initialState');

  const intl = useIntl();

  const fetchUserInfo = async (loginGuider:any) => {
		if (loginGuider) {
			await setInitialState((s) => ({
				...s,
				currentUser: loginGuider.user,
			}));

			sessionStorage.setItem(UserConstant.USER_TOKEN_KEY, loginGuider.token)
		}
	};

  const handleSubmit = async (values: API.LoginParams) => {
		// 登录
		let host=window.location.protocol + "//" + window.location.host
		const loginResult=await login({...values});
		message.success("登录成功");
		await fetchUserInfo(loginResult);
		const {query} = history.location;
		const {loginSuccessUrl} = query as {loginSuccessUrl: string};

		let resourceUrl="/"
		if (loginSuccessUrl){
			resourceUrl=loginSuccessUrl.replace(host, '')
		}
		history.push(resourceUrl || RoutesConstant.ROOT_PATH);
		return;
  };

  return (
    <div className={styles.container}>
      <div className={styles.lang} data-lang>
        {SelectLang && <SelectLang />}
      </div>
      <div className={styles.content}>
        <LoginForm
          logo={<img alt="logo" src="/logo.png" />}
          title="A CDC"
          subTitle="欢迎使用 ACDC 平台"
          initialValues={{
            autoLogin: true,
          }}
          onFinish={async (values) => {
            await handleSubmit(values as API.LoginParams);
          }}
        >
          <Tabs>
            <Tabs.TabPane
              key="account"
              tab={intl.formatMessage({
                id: 'pages.login.accountLogin.tab',
                defaultMessage: '账户密码登录',
              })}
            />
          </Tabs>

          {(
            <>
              <ProFormText
                name="username"
                fieldProps={{
                  size: 'large',
                  prefix: <UserOutlined className={styles.prefixIcon} />,
                }}
								placeholder={'用户名: admin/admin@acdc.cn'}
                rules={[
                  {
                    required: true,
                    message: "请输入用户名!"
                  },
                ]}
              />
              <ProFormText.Password
                name="password"
                fieldProps={{
                  size: 'large',
                  prefix: <LockOutlined className={styles.prefixIcon} />,
                }}
                placeholder={  '密码: ' }
                rules={[
                  {
                    required: true,
                    message: "请输入密码！"
                  },
                ]}
              />
            </>
          )}
          <div
            style={{
              marginBottom: 24,
            }}
          >
						{/*
            <ProFormCheckbox noStyle name="autoLogin">
              <FormattedMessage id="pages.login.rememberMe" defaultMessage="自动登录" />
            </ProFormCheckbox>
						*/}
          </div>
        </LoginForm>
      </div>
    </div>
  );
};

export default Login;
