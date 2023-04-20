// @ts-ignore
/CONSTANT* eslint-disable */

/**
***********************A CDC service********************************
*/

/** 处理每行的过滤条件 */
const NONE_FIELD: string = '__none\tstring'
export function handleFieldMappingItem(mappings: API.ConnectionColumnConf[]) {
  let newItems: API.ConnectionColumnConf[] = [];
  mappings.forEach((val, index, arr) => {
    let sourceField = (!val.sourceFieldFormat || '' == val.sourceFieldFormat)
      ? NONE_FIELD
      : val.sourceFieldFormat

    newItems.push({
      ...val,
      sourceFieldFormat: sourceField
    })

  })
  return newItems;
}

export function verifyPrimaryKey(
  mappings: API.ConnectionColumnConf[],
  sinkDataSystemType: string
): boolean {

  let valid: boolean = false;
  let sourceExistsPriOrUni: boolean = false;

  mappings?.forEach((record, index, arr) => {
    const srcFiledStruct: string[] = (!record) ? [] : record!.sourceFieldFormat!.split("\t");
    const sinkFiledStruct: string[] = record!.sinkFieldFormat!.split("\t");

    const sinkIsPri: boolean = (sinkFiledStruct.length == 3 && sinkFiledStruct[2] == 'PRI')
    const sourceIsPriOrUni: boolean = (srcFiledStruct.length == 3 && (srcFiledStruct[2] == 'PRI' || srcFiledStruct[2] == 'UNI'))

    // HIVE 类型的只要上游存在主键或唯一索引即可
    if (sourceIsPriOrUni) {
      sourceExistsPriOrUni = true;
    }

    // 字段映射: 只要存在一条 sink 的 pk 与 source 的 pk 或 uni key 匹配即可
    if (sinkIsPri && sourceIsPriOrUni) {
      valid = true
    }
  })

  return (sinkDataSystemType == 'HIVE' || sinkDataSystemType == 'KAFKA') ? sourceExistsPriOrUni : valid
}
