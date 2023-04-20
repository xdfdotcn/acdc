import React, { useRef, useState } from 'react';
import type { EditableFormInstance } from '@ant-design/pro-table';
import { getConnectionDetail } from '@/services/a-cdc/api';
import type { ConnectionColumnConfProps } from '../connection/components/ConnectionColumnConfNb';
import ConnectionColumnConfNb from '../connection/components/ConnectionColumnConfNb';

const Nb: React.FC = () => {
  const [connectionColumnConfState, setConnectionColumnConfState] = useState<ConnectionColumnConfProps>();
  const editorFormRef = useRef<EditableFormInstance<API.ConnectionColumnConf>>();
  return (
    <div>
      <a onClick={async () => {
        alert("你点我锕")
        const detail: API.ConnectionDetail = await getConnectionDetail(12)
        setConnectionColumnConfState({
          displayDataSource: detail.connectionColumnConfigurations!,
          originalDataSource: detail.connectionColumnConfigurations!,
          canEdit: true,
          canDelete: false,
          sinkDataSystemType: detail.sinkDataSystemType!,
          sourceDataCollectionId: detail.sourceDataCollectionId,
        })
        const rows = editorFormRef.current?.getRowsData?.();
        console.log("🚀 ~ file: index.tsx:24 ~ <aonClick={ ~ rows:", rows)
        
      }}>你点我锕</a>
      <p></p>

      <a onClick={async () => {
        alert("你再点我锕")
        const rows = editorFormRef.current?.getRowsData?.();
        console.log("🚀 ~ file: index.tsx:33 ~ <aonClick={ ~ rows:", rows)

        let testArr=['yang2','yang3','yang1','yang2'];

        // 测试排序的功能，如果排序好用，基本功能就算完成了

        testArr.sort((n1,n2)=>{
          // n1.localeCompare(n2,'zh-CN')
          return n1.localeCompare(n2)
        })

        console.log("🚀 ~ file: index.tsx:34 ~ <aonClick={ ~ testArr:", testArr)

      }}>你再点我锕</a>


      <ConnectionColumnConfNb
        columnConfProps={{ ...connectionColumnConfState }}
        editorFormRef={editorFormRef}
      />
    </div>
  )
};

export default Nb;
