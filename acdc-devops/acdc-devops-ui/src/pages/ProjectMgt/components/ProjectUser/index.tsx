import {editProjectUser, queryProjectUser, queryUser} from '@/services/a-cdc/api';
import {EditOutlined} from '@ant-design/icons';
import ProForm from '@ant-design/pro-form';
import {EditableFormInstance, EditableProTable, ProColumns} from '@ant-design/pro-table';
import {Select,Modal, message} from 'antd';
import { useRef, useState } from 'react';
import {useModel} from 'umi';
const {confirm} = Modal;

const ProjectUser: React.FC = () => {

	// 数据流model,接受来自任何页面的传参
	const {projectUserModel} = useModel('ProjectUserModel')

	// 编辑表格启动编辑模式,固定的state触发
	const [editableKeys, setEditableRowKeys] = useState<React.Key[]>(() => []);

	/**
		1. 编辑表格,增加record记录需要使用唯一key,每次都需要累加,保证不重复
		2. 初始化值为表格中所有记录中record id 最大的值
		3. 每次增加一条记录都需要累加此值
		4. 每次添加record记录的时候默认返回最新的值,保证动态添加一定会有不重复的唯一
		key
	*/

	const [recordId, setRecordId] = useState<number>(-99)

	// 使用form ref 获取表格中的数据,这样使用起来更加方便,不用额外使用state
	const editorFormRef = useRef<EditableFormInstance<API.AcdcUser>>();

	// 下拉菜单初始化,过滤掉表格中已经存在的用户
	const [userSelect, setUserSelect] = useState<API.AcdcUser[]>(() => []);

	// 只有有state的更改就会触发
	//useEffect(() => {
	//});

	// 发生在首次挂载(mount),更新state不会再次触发
	//useEffect(() => {
	//}, []);

	/**页面列表数据加载完成,更新下拉菜单数据*/
	const initUserSelectDataAndMaxRecordId = async (items: API.AcdcUser[]) => {
		//1. 请求数据
		let userSelectResult: API.AcdcUser[] = await queryUser({})
		let userSelectData = userSelectResult
		const tableUserMap: Map<string, string> = new Map([]);
		items!.forEach((val, idx, arr) => {
			tableUserMap.set(val.email!, val.email!)
		})

		//2. 过滤数据
		let newUserSelect: API.AcdcUser[] =
			userSelectData!.filter(
				(item) => {return item.email !== tableUserMap.get(item.email)}
			)

		let maxId = 0
		for (let i = 0; i < items.length; i++) {
			if (maxId < items[i].id!) {
				maxId = items[i].id!
			}
		}
		//3. 更新state
		setUserSelect(newUserSelect)

		maxId = maxId + 1
		setRecordId(maxId)
	}

	const submitForm = async () => {
		confirm({
			title: '确定提交吗',
			icon: <EditOutlined />,
			content: '修改用户',
			async onOk() {
				const talbeUsers = editorFormRef.current?.getFieldValue('table') as API.AcdcUser[];
				let body: API.AcdcUser[] = [];
				talbeUsers.forEach((val, idx, arr) => {
					body.push({
						email: val.email
					})
				}
				)
				editProjectUser(body, {projectId: projectUserModel.projectId})
				message.info("修改成功")
			},
			onCancel() {},
		});
	}

	// 编辑表格列声明
	const columns: ProColumns<API.AcdcUser>[] = [
		{
			title: '用户',
			dataIndex: 'email',
			formItemProps: () => {
				return {
					rules: [{required: true, message: '此项为必填项'}],
				};
			},
			width: '30%',
			renderFormItem: (record, {defaultRender, ...rest}, form) => {
				return (
					<Select
						showSearch
						placeholder="请选择"
						style={{minWidth: 200}}
						optionFilterProp="children"
						onSelect={(item) => {}}
						virtual={false}
					>
						{
							userSelect!.map((item) =>
								<Option value={item.email} key={item.email}> {item.email} </Option>)
						}
					</Select>
				)
			},
		},

		{
			title: '名称',
			width: "46%",
			editable: false,
			render: (dom, entity) => {
				return (
					<>{entity.email == projectUserModel.ownerEmail ? '项目负责人' : '普通用户'}</>
				);
			},
		},

    {
      title: '操作',
      valueType: 'option',
      width: 200,
      render: (text, record, _, action) => [
				<a
					key={"delete" + record.id}
					onClick={() => {
						if (record.email == projectUserModel.ownerEmail) {
							message.warn("项目负责人不能删除")
							return
						}
						confirm({
							title: '确定删除吗',
							icon: <EditOutlined />,
							content: '删除用户',
							async onOk() {
								// 更新下拉内容,增加已经删除的用户
								const newUserSelect = userSelect
								newUserSelect.push(record)
								setUserSelect(newUserSelect)

								// 页面效果,列表中移除掉当前需要删除的item
								const currentTableItems = editorFormRef.current?.getFieldValue('table') as API.AcdcUser[];
								let newTableItems = currentTableItems.filter((item) => item.id !== record?.id)
								editorFormRef.current?.setFieldsValue({
									table: newTableItems,
								});

								message.success('操作成功');
							},
							onCancel() {},
						});
					}}
				>
					删除
				</a>,
      ],
    },
  ];

	return (
		<div>
			<>
				<ProForm<{
					table: API.AcdcUser[];
				}>
					onFinish={() => {
						submitForm()
					}}
					validateTrigger="onBlur"
				>
					<EditableProTable<API.AcdcUser>
						rowKey="id"
						scroll={{
							x: 960,
						}}
						editableFormRef={editorFormRef}
						maxLength={50}
						name="table"
						recordCreatorProps={
							{
								position: 'top',
								record: (__, records) => {
									return {id: recordId}
								},
							}
						}
						columns={columns}
						// TODO 增加请求参数
						params={{
							projectId: projectUserModel.projectId
						}}
						request={queryProjectUser}
						onLoad={(items) => {initUserSelectDataAndMaxRecordId(items)}}
						editable={{
							type: 'single',
							editableKeys,
							onChange: setEditableRowKeys,
							actionRender: (row, config, defaultDom) => {
								return [
									defaultDom.save,
									defaultDom.delete || defaultDom.cancel,
								];
							},

							onSave: async (index, cur, old) => {
								let newUserSelect = userSelect.filter(((item) => {return item.email !== cur.email}))
								setUserSelect(newUserSelect)
								// 更新最大的recordId,防止id key 重复
								setRecordId(recordId + 1)
							},
						}}
					/>
				</ProForm>
			</>
		</div>
	)
};

export default ProjectUser;
