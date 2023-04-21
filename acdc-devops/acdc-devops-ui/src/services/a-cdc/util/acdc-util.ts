export function isEmptyString(str?: string): boolean {
  if (!str
    || str === ''
  ) {
    return true;
  }
  return false;
}

export function isEmptyArray(arr?: any[]): boolean {
  if (!arr
    || arr.length <= 0
  ) {
    return true;
  }
  return false
}
