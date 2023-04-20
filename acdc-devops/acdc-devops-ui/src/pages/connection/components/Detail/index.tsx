import React from 'react';
import ProCard from "@ant-design/pro-card";
import ConnectionColumnConf from '../ConnectionColumnConf';
//import ColumnConfiguration from "@/pages/connection/components/ColumnConfiguration";

const ConnectionDetail: React.FC<{connectionDetail: API.ConnectionDetail, index: number, from: string}> = ({connectionDetail, index,from}) => {
  return (
    <ProCard split="horizontal">
      <ProCard split="vertical">
        <ProCard title="源项目">
          {connectionDetail.sourceProjectName}
        </ProCard>
        <ProCard title="源数据系统">
          {connectionDetail.sourceDataSystemType}
        </ProCard>
        <ProCard title="数集群">
          {connectionDetail.sourceDataSystemClusterName}
        </ProCard>
        <ProCard title="源 database">
          {connectionDetail.sourceDatabaseName}
        </ProCard>
        <ProCard title="源 table">
          {connectionDetail.sourceDataCollectionName}
        </ProCard>
        <ProCard title="目标项目">
          {connectionDetail.sinkProjectName}
        </ProCard>
        <ProCard title="目标数据系统">
          {connectionDetail.sinkDataSystemType}
        </ProCard>
        <ProCard title="目标集群">
          {connectionDetail.sinkDataSystemClusterName}
        </ProCard>
        <ProCard title="目标 database">
          {connectionDetail.sinkDatabaseName}
        </ProCard>
        <ProCard title="目标数据集（table、topic）">
          {connectionDetail.sinkDataCollectionName}
        </ProCard>
      </ProCard>
      <ProCard title="字段映射及过滤规则">
        <ConnectionColumnConf
          columnConfProps={{
            displayDataSource: connectionDetail.connectionColumnConfigurations,
            originalDataSource: connectionDetail.connectionColumnConfigurations!,
            sinkDataSystemType: connectionDetail.sinkDataSystemType,
            canEdit: false,
            canDelete: false,
          }}
        />
      </ProCard>
    </ProCard>
  );
};

export default ConnectionDetail;
