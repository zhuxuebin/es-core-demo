package com.sf.es.core.common;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 * Created by xuery on 2018/5/2.
 */
public class EsClientUtils {

    private static final Logger logger = LoggerFactory.getLogger(EsClientUtils.class);

    private EsClientUtils(){

    }

    public static TransportClient getTransportClient() {
        try {
            Settings settings = Settings.builder()
                    .put("cluster.name", "my-es")
                    .build();

            TransportClient client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
            return client;
        } catch (Exception e){
            logger.error("获取TransportClient出错",e);
            return null;
        }
    }
}
