import React from 'react';
import { PageContainer } from '@ant-design/pro-layout';
import { Card, Alert, Typography } from 'antd';
import { useIntl, FormattedMessage } from 'umi';
import styles from './index.less';

export default (): React.ReactNode => {
  const intl = useIntl();
  return (
    <PageContainer>
      <Card>
        <Alert
          message='关键字说明'
          type="success"
          showIcon
          banner
          style={{
            margin: -12,
            marginBottom: 24,
          }}
        />
        <p><b>source: </b> 数据源，数据事件产生的系统</p>
        <p><b>sink: </b> 数据目标，数据事件被同步到的系统</p>
        <p><b>data system: </b> 数据系统，提供数据存储服务的系统。如：MySQL、Kafka</p>
        <p><b>data set: </b> 数据集，数据系统中的数据集合，如 MySQL 中的 table，Kafka 中的 topic</p>
        <p><b>data event: </b> 数据事件，对应数据集中的数据发生的一次变更（增删改）</p>
        <p><b>data event sync: </b> 数据事件同步，将 source 数据集中的数据变更操作在 sink 数据集中执行的过程</p>
        <p><b>connection: </b> 数据链路，表粒度，是 source、sink 数据集间的数据传输通道（如：从 source 的一张表到 sink 的一张表）</p>
      </Card>
      <Card>
        <Alert
          message='如何建立一条数据链路'
          type="success"
          showIcon
          banner
          style={{
            margin: -12,
            marginBottom: 24,
          }}
        />
        <p>1. 点击 ACDC 左侧菜单栏 "链路申请" 按钮</p>
        <p>2. 在打开的界面中，点击右上角 "新增链路" 按钮</p>
        <p>3. 按照步骤依次选择 source、sink 的项目及数据系统相关信息（库、表、实例）</p>
        <p>注：（若第 3 步中无法找到 source、sink 的所属系统，请联系 cdc 团队成员：于峰13，杨中奎，戈震，朱润华）</p>
        <p>4. 配置 source、sink 的字段映射关系，并确认链路信息</p>
        <p>5. 若还需建立其他链路，请重复 2、3、4 步骤</p>
        <p>6. 在 "链路申请" 页面下方的文本框中输入链路申请理由，并点击提交</p>
        <p>7. 经过数据源所属系统负责人、DBA 老师审批后，链路建立完成（会有邮件通知）</p>
        <p>8. 点击 ACDC 左侧菜单栏的 "链路管理" 按钮，并在界面中找到刚才的链路信息，点击操作栏中 "启动" 按钮</p>
        <p>9. 待 "链路管理" 页面中，该链路的状态变为 "运行中"，即为数据链路启动完成。这时若 source 中的数据集被触发了数据事件，该事件会被同步至 sink 对应的数据集中</p>
      </Card>
    </PageContainer>
  );
};
