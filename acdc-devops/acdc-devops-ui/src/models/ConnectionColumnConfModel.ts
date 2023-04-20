/** 字段映射公共组件 */

import {useState, version} from 'react'

export default () => {

  const [connectionColumnConfModel, setConnectionColumnConfModel] = useState<API.ConnectionColumnConfModel>({
    displayData: [],
    currentData: [],
    originalData: [],
    version: 1,
    canEdit: false,
    canDelete: false,
  })

  const setDisplayData = (displayData: API.ConnectionColumnConf[]) => {
    setConnectionColumnConfModel({...connectionColumnConfModel, displayData});
  }

  const setOriginalData = (originalData: API.ConnectionColumnConf[]) => {
    setConnectionColumnConfModel({...connectionColumnConfModel, originalData});
  }

  const setCurrentData = (currentData: API.ConnectionColumnConf[]) => {
    setConnectionColumnConfModel({...connectionColumnConfModel, currentData});
  }

  const setConnectionColumnConfModelData = (
    dataModel: API.ConnectionColumnConfModel
  ) => {

    let newOriginalData: API.ConnectionColumnConf[] = []
    let originalData = dataModel.originalData!
    originalData.forEach((record, _index, _arr) => {
      newOriginalData.push(record)
    })

    let newDisplayData: API.ConnectionColumnConf[] = []
    let displayData = dataModel.displayData!
    displayData.forEach((record, _index, _arr) => {
      newDisplayData.push(record)
    })

    setConnectionColumnConfModel({
      ...dataModel,
      originalData: newOriginalData,
      displayData: newDisplayData,
      version: connectionColumnConfModel.version! + 1
    }
    )
  }
  // 已经测试过,...赋值,后面相同的key可以覆盖之前的
  // eg: {...connectorModel,refreshVersion:1}
  return {
    connectionColumnConfModel,
    setConnectionColumnConfModel,
    setOriginalData,
    setCurrentData,
    setConnectionColumnConfModelData,
    setDisplayData,
  }
}
