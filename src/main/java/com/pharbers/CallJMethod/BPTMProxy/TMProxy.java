package com.pharbers.CallJMethod.BPTMProxy;

import com.pharbers.BPMgoSpkProxy.BPMgoSpkProxyImpl;

public class TMProxy {
    public String BPTMUCBPreCal(String perposalid,
                                String projectid,
                                String periodid,
                                Number phase) {
        return BPMgoSpkProxyImpl.loadDataFromMgo2Spark(perposalid, projectid, periodid, phase.intValue());
    }
}
