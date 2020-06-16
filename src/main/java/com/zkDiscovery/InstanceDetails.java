package com.zkDiscovery;

import org.codehaus.jackson.map.annotate.JsonRootName;

/**
 * @ClassName InstanceDetails
 * @Description 实例详情类，服务实例的具体信息，还会更详细
 * @Author 贺楚翔
 * @Date 2020-06-08 10:27
 * @Version 1.0
 **/
@JsonRootName("details")
public class InstanceDetails {
    private String description;

    public InstanceDetails() {
    }

    public InstanceDetails(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
