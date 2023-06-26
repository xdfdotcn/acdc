import { message } from 'antd';
import { DataSystemTypeConstant } from '../constant/DataSystemTypeConstant';
import { isEmptyArray } from '../util/acdc-util';

// @ts-ignore
/CONSTANT* eslint-disable */;

export function verifyUk(
  columnConfList: API.ConnectionColumnConf[],
  sinkDataSystemType: string,
): boolean {
  let valid: boolean = false;
  let existsSrcUks: boolean = false;
  for (let record of columnConfList) {
    let srcUks: string[] | undefined = record.sourceColumnUniqueIndexNames;
    let sinkUks: string[] | undefined = record.sinkColumnUniqueIndexNames;
    if (!isEmptyArray(srcUks)) {
      existsSrcUks = true;
    }
    if (!isEmptyArray(srcUks) && !isEmptyArray(sinkUks)) {
      valid = true;
    }
  }

  if (
    sinkDataSystemType == DataSystemTypeConstant.HIVE ||
    sinkDataSystemType == DataSystemTypeConstant.KAFKA ||
    sinkDataSystemType == DataSystemTypeConstant.ELASTIC_SEARCH
  ) {
    return existsSrcUks;
  }

  if (
    sinkDataSystemType == DataSystemTypeConstant.TIDB ||
    sinkDataSystemType == DataSystemTypeConstant.MYSQL
  ) {
    return valid;
  }

  return false;
}

export function verifyUKWithShowMessage(
  columnConfList: API.ConnectionColumnConf[],
  sinkDataSystemType: string,
): boolean {
  let verifyPass = verifyUk(columnConfList, sinkDataSystemType);

  if (
    sinkDataSystemType == DataSystemTypeConstant.HIVE ||
    sinkDataSystemType == DataSystemTypeConstant.KAFKA ||
    sinkDataSystemType == DataSystemTypeConstant.ELASTIC_SEARCH
  ) {
    if (!verifyPass) {
      message.warn('源表不存在唯一索引字段');
    }
  }

  if (
    sinkDataSystemType == DataSystemTypeConstant.TIDB ||
    sinkDataSystemType == DataSystemTypeConstant.MYSQL
  ) {
    if (!verifyPass) {
      message.warn('至少包含一组唯一索引字段的映射关系');
    }
  }

  return verifyPass;
}
