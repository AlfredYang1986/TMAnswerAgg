package com.pharbers.CallJMethod.BPTMProxy;

import com.pharbers.BPMgoSpkProxy.BPMgoSpkProxyImpl;

public class TMProxy {
    public String BPTMUCBPreCal(String proposalid,
                                String projectid,
                                String periodid,
                                Number phase) {

        return BPMgoSpkProxyImpl.loadDataFromMgo2Spark(proposalid, projectid, periodid, phase.intValue());
    }

    public void BPTMUCBPostCal(String jobid,
                                 String proposalid,
                                 String projectid,
                                 String periodid,
                                 Number phase) {

        BPMgoSpkProxyImpl.loadDataFromSpark2Mgo(jobid, proposalid, projectid, periodid, phase.intValue());
    }
}
