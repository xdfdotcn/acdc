import React from 'react';
import {PageContainer} from '@ant-design/pro-layout';
import {Alert, Card, Typography} from 'antd';
import {useIntl} from 'umi';
import styles from './Welcome.less';

const CodePreview: React.FC = ({ children }) => (
  <pre className={styles.pre}>
    <code>
      <Typography.Text copyable>{children}</Typography.Text>
    </code>
  </pre>
);

export default (): React.ReactNode => {
  const intl = useIntl();
  return (
    <PageContainer>
      <Card>
        <Alert
          message='欢迎使用 ACDC 平台'
          type="success"
          showIcon
          banner
          style={{
            margin: -12,
            marginBottom: 24,
          }}
        />
				<div>
          <p>ACDC 是信管架构部出品的第二代 cdc 产品，定位是以 DevOps 的方式为研发团队提供端到端的实时数据同步能力。</p>
          <p>初次使用，请查看 <a href="/doc/quickstart">快速开始</a>。</p>
        </div>
        <Card>
          <p>目前支持的数据源如下：</p>
          <p><b>MySQL</b></p>
          <p><b>TiDB</b></p>
        </Card>
        <Card>
          <p>目前支持的数据目标如下：</p>
          <p><b>MySQL</b></p>
          <p><b>TiDB</b></p>
          <p><b>Hive</b></p>
          <p><b>Kafka</b></p>
          <p><b>StarRocks</b></p>
          <p><b>SQLServer</b></p>
          <p><b>Oracle</b></p>
        </Card>
      </Card>
    </PageContainer>
  );
};
