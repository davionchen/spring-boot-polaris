/*
 * Copyright 2013-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.polarismesh.boot.discovery.feign;

import com.tencent.polaris.api.core.ConsumerAPI;
import com.tencent.polaris.api.pojo.Instance;
import com.tencent.polaris.api.rpc.GetOneInstanceRequest;
import com.tencent.polaris.api.rpc.InstancesResponse;
import feign.Request;
import feign.RequestTemplate;
import org.springframework.util.StringUtils;

public class PolarisTarget<T> implements feign.Target<T> {

    private final ConsumerAPI consumerAPI;

    private final Class<T> clazz;

    private final PolarisFeignOptions polarisFeignOptions;

    public PolarisTarget(ConsumerAPI consumerAPI, Class<T> clazz,
            PolarisFeignOptions polarisFeignOptions) {
        this.consumerAPI = consumerAPI;
        this.clazz = clazz;
        this.polarisFeignOptions = polarisFeignOptions;
    }

    @Override
    public Class<T> type() {
        return clazz;
    }

    @Override
    public String name() {
        return polarisFeignOptions.getService();
    }

    @Override
    public String url() {
        return null;
    }

    @Override
    public Request apply(RequestTemplate input) {
        Instance instance = choose();
        String schema = getSchema(instance);
        String url = String.format("%s://%s:%s", schema,
                instance.getHost(), instance.getPort());
        input.header(PolarisFeignConst.HEADER_NAMESPACE, polarisFeignOptions.getNamespace())
                .header(PolarisFeignConst.HEADER_SERVICE, polarisFeignOptions.getService());

        input.target(url);
        return input.request();
    }

    private Instance choose() {
        String namespace = polarisFeignOptions.getNamespace();
        String service = polarisFeignOptions.getService();
        GetOneInstanceRequest getInstancesRequest = new GetOneInstanceRequest();
        getInstancesRequest.setNamespace(namespace);
        getInstancesRequest.setService(service);
        getInstancesRequest.setMetadata(polarisFeignOptions.getMetadata());
        InstancesResponse instances = consumerAPI.getOneInstance(getInstancesRequest);
        if (instances.getInstances().length == 0) {
            throw new RuntimeException(
                    String.format("instance not found for service %s, namespace %s", service, namespace));
        }
        return instances.getInstances()[0];
    }

    /**
     * 获取协议
     * @param instance
     */
    private String getSchema(Instance instance){
        if(StringUtils.hasText(instance.getProtocol())){
            return instance.getProtocol();
        }else {
            return polarisFeignOptions.getScheme();
        }
    }

}
