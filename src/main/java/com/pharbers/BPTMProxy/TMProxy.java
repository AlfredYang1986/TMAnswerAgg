package com.pharbers.BPTMProxy;

import com.pharbers.BPMgoSpkProxy.BPMgoSpkProxyImpl;

public class TMProxy {
    public String BPTMUCBPreCal(String perposalid,
                                String projectid,
                                String periodid,
                                int phase) {
        return BPMgoSpkProxyImpl.loadDataFromMgo2Spark(perposalid, projectid, periodid, phase);
    }
}
