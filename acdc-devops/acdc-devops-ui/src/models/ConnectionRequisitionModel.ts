import { useState } from 'react'


/**
 * Connection requisition model.
 */
export default () => {

  const [connectionRequisitionDetail, setConnectionRequisitionDetail] = useState<API.ConnectionRequisitionDetail>({})

  return {
    connectionRequisitionDetail,
    setConnectionRequisitionDetail,
  }
}
