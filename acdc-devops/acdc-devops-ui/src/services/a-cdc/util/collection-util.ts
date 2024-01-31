export class AcdcCollections {
  static convertMapToList<K, V>(map: Map<K, V>): V[] {
    const values: V[] = [];
    map.forEach((v) => {
      values.push({ ...v });
    });
    return values;
  }
}
