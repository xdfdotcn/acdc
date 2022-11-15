// @ts-ignore
/CONSTANT* eslint-disable */

/**
***********************A CDC service********************************
*/

/** 处理每行的过滤条件 */
const NONE_FIELD: string = '__none\tstring'
export function handleFieldMappingItem(mappings: API.FieldMappingListItem[]) {
	let newItems: API.FieldMappingListItem[] = [];
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
	mappings: API.FieldMappingListItem[],
	sinkDataSystemType: string
): boolean {

	let valid: boolean = true;
	let existsSrcPK: boolean = false;

	mappings.forEach((record, index, arr) => {
		let srcFiledStruct: string[] = (!record) ? [] : record!.sourceFieldFormat!.split("\t");
		let sinkFiledStruct: string[] = record!.sinkFieldFormat!.split("\t");

		let sinkFieldCode: number =
			(sinkFiledStruct.length == 3 && sinkFiledStruct[2] == 'PRI')
				? 1 : 2

		let srcFieldCode: number =
			(srcFiledStruct.length == 3 && srcFiledStruct[2] == 'PRI')
				? 1 : 2

		// HIVE 类型的只要上游存在主键即可
		if (srcFieldCode == 1) {
			existsSrcPK = true;
		}

		// 字段映射:必须同为主键或者普通字段
		if ((sinkFieldCode ^ srcFieldCode) != 0) {
			valid = false
		}
	})

	return (sinkDataSystemType == 'HIVE' || sinkDataSystemType == 'KAFKA') ? existsSrcPK : valid
}
