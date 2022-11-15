import React, {useEffect, useState} from 'react';
import {useModel} from 'umi';
import {Descriptions} from 'antd';
import ProCard from '@ant-design/pro-card';
import {getProject} from '@/services/a-cdc/api';
const ProjectDetail: React.FC = () => {
	// 项目详情页面的数据model
	const {projectDetailModel} = useModel('ProjectDetailModel')

	const [projectDetail, setProjectDetail] = useState<API.Project>({});

	useEffect(() => {
		initData();
	}, [projectDetailModel.projectId]);


	const initData = async () => {
		let project: API.Project = await getProject({projectId: projectDetailModel.projectId})
		setProjectDetail({
			id: projectDetailModel.projectId,
			name: project.name,
			description: project.description,
			ownerEmail: project.ownerEmail
		})
	}

	return (
		<>
			<ProCard
				title="项目信息"
				headerBordered
				bordered
				onCollapse={(collapse) => {}}
			>
				<Descriptions column={1}>
					<Descriptions.Item label="项目名称">
						{projectDetail.name}
					</Descriptions.Item>
					<Descriptions.Item label="项目负责人">
						{projectDetail.ownerEmail}
					</Descriptions.Item>
					<Descriptions.Item label="项目描述">
						{projectDetail.description}
					</Descriptions.Item>
				</Descriptions>
			</ProCard>
		</>
	)
};
export default ProjectDetail;
