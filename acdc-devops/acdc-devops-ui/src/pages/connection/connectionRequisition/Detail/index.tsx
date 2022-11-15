import React from 'react';
import ConnectionDetail from "@/pages/connection/components/Detail";
import ProCard from "@ant-design/pro-card";

const ConnectionRequisitionDetail: React.FC<{connectionRequisitionDetail: API.ConnectionRequisitionDetail}> = ({connectionRequisitionDetail}) => {
  return (
    <ProCard split="horizontal">
      <ProCard>
        <ProCard title="当前审批状态">
          {connectionRequisitionDetail.state}
        </ProCard>
        <ProCard title="申请人">
          {connectionRequisitionDetail.connections?.at(0).userEmail}
        </ProCard>
        <ProCard title="申请理由">
          {connectionRequisitionDetail.description}
        </ProCard>
        <ProCard title="数据源审批人">
          {connectionRequisitionDetail.sourceApproverEmail}
        </ProCard>
        <ProCard title="数据源审批结果">
          {connectionRequisitionDetail.sourceApproveResult}
        </ProCard>
        <ProCard title="DBA 审批人">
          {connectionRequisitionDetail.dbaApproverEmail}
        </ProCard>
        <ProCard title="DBA 审批结果">
          {connectionRequisitionDetail.dbaApproveResult}
        </ProCard>
      </ProCard>

      <ProCard split="horizontal">
        {
          connectionRequisitionDetail.connections?.map((item, index) => {
            return <ProCard title={"第 " + (index + 1) + " 条链路"}
						key={index}><ConnectionDetail key={index} index={index}
						connectionDetail={{...item}} from='approve' /></ProCard>
          })
        }
      </ProCard>
    </ProCard>
  );
};

export default ConnectionRequisitionDetail;
