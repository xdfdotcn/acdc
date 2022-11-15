import {useParams} from 'react-router-dom';
import {doConnectionRequisitionApprove, getConnectionRequisitionDetail} from "@/services/a-cdc/api";
import ConnectionRequisitionDetail from "@/pages/connection/connectionRequisition/Detail";
import {useEffect, useRef, useState} from "react";
import {ProCard} from "@ant-design/pro-card";
import {Button, Input, InputRef, Spin,Modal, message} from "antd";
import {EditOutlined} from '@ant-design/icons';
import { history } from 'umi';
const {confirm} = Modal;

const { TextArea } = Input;

export default () => {
  const {connectionRequisitionId} = useParams()
  const [connectionRequisitionDetail, setConnectionRequisitionDetail] = useState<API.ConnectionRequisitionDetail>({})
  const [loading, setLoading] = useState(true);
  const [approveResult, setApproveResult] = useState("");
	const [reload,setReload] =useState(true);

  const doGetConnectionRequisitionDetail = async () => {
    const detail = await getConnectionRequisitionDetail(connectionRequisitionId)
    setConnectionRequisitionDetail(detail);
    setLoading(false);
  }

  useEffect(() => {
    doGetConnectionRequisitionDetail();
  }, []);

  const doApprove = (result: boolean) => {
		let hint = result ? '确定通过审批?' : '确定拒绝审批?'
		confirm({
			title: hint,
			icon: <EditOutlined />,
			content: '链路审批',
			async onOk() {
				if (!approveResult || approveResult == '') {
					message.warn("请输入审批意见")
					return false
				}
				await doConnectionRequisitionApprove(connectionRequisitionId, result, approveResult);
				history.go(0)
			},
			onCancel() {},
		});
	}

  return (
    <Spin spinning={loading}>
      <ProCard
        title="新链路申请审批"
        bordered
        headerBordered
        direction="column"
        gutter={[16]}
        layout="center"
        split="horizontal"
      >
        <ConnectionRequisitionDetail connectionRequisitionDetail={connectionRequisitionDetail} />
        {
          (connectionRequisitionDetail.state == 'APPROVING' || connectionRequisitionDetail.state == 'SOURCE_OWNER_APPROVING' || connectionRequisitionDetail.state == 'DBA_APPROVING')? (
            <ProCard layout="center" split="horizontal">
              <ProCard>
                <TextArea rows={4} placeholder="请输入审批意见" maxLength={200} onChange={e => setApproveResult(e.target.value)} />
              </ProCard>

              <ProCard layout='center' colSpan={8}>
                <ProCard colSpan={4}>
                  <Button size='large' onClick={() => doApprove(true)}>通过</Button>
                </ProCard>
                <ProCard colSpan={4}>
                  <Button size='large' danger onClick={() => doApprove(false)}>拒绝</Button>
                </ProCard>
              </ProCard>
            </ProCard>
          ) : (
            <ProCard/>
          )
        }
      </ProCard>
    </Spin>
  )
};
