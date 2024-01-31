package cn.xdf.acdc.devops.service.process.widetable;

public interface IdGenerator {
    
    Long NULL_ID = null;
    
    /**
     * Get id.
     *
     * @return id
     */
    Long id();
    
    class MemoryIdGenerator implements IdGenerator {
        
        private Long beginId = 1L;
        
        @Override
        public Long id() {
            if ((Long.MAX_VALUE - beginId) <= 0) {
                throw new IllegalStateException("Value out of bounds");
            }
            
            return ++beginId;
        }
    }
    
    class DbIdGenerator implements IdGenerator {
        
        @Override
        public Long id() {
            return IdGenerator.NULL_ID;
        }
    }
}
