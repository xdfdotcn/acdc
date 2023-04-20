import {useState, useCallback} from 'react'


/** connector 申请单数据model*/
export default () => {

  const [applyInfoModel, setApplyInfoModel] = useState<API.ConnectionApplyModel>({})

  return {
    applyInfoModel,
    setApplyInfoModel,
  }
}
