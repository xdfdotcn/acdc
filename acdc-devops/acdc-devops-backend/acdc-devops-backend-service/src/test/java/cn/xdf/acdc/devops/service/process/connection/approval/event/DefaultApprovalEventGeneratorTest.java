package cn.xdf.acdc.devops.service.process.connection.approval.event;

import cn.xdf.acdc.devops.service.BaseTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultApprovalEventGeneratorTest extends BaseTest {

    private DefaultApprovalEventGenerator defaultApprovalEventGenerator;

    @Before
    public void setup() {
        defaultApprovalEventGenerator = new DefaultApprovalEventGenerator();
    }

    @Test
    public void testDoGenerateApprovalEvent() {
        // TODO
    }
}
