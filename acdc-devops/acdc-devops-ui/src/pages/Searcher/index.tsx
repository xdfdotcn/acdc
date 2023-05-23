import { DataSystemTypeConstant } from '@/services/a-cdc/constant/DataSystemTypeConstant';
import React, { useState } from 'react';
import DcSearcher, { DataCollection } from '../connection/components/DcSearcher';
const Searcher: React.FC = () => {
  const [dataCollectionsState, setDataCollectionsState] = useState<DataCollection[]>([]);
  return (
    <div>
      <DcSearcher
        reqPageSize={20}
        multipleChoice={true}
        includedDataSystemTypes={[DataSystemTypeConstant.TIDB, DataSystemTypeConstant.MYSQL]}
        onSelect={(dcList) => {
          console.log('父节点获取到的选中数据集,dcList: ', dcList);
          console.log('父节点获取到的选中数据集,dataCollectionsState: ', dataCollectionsState);
          setDataCollectionsState(dcList)
        }}
      />
    </div>
  );
};

export default Searcher;
