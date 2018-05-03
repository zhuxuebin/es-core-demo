package com.sf.es.core.first;


import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Created by xuery on 2018/4/8.
 */
public class EmployeeCURDApp {

    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "my-es")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        //创建document
//        createEmployee(client);
//        updateEmployee(client);
        queryEmployee(client);
//        deleteEmployee(client);
        client.close();
    }

    private static void createEmployee(TransportClient client) throws IOException {
        IndexResponse response = client.prepareIndex("company", "employee", "1")
                .setSource(
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .field("name", "jack")
                                .field("age", 27)
                                .field("position", "technique")
                                .field("join_date", "2017-01-01")
                                .field("salary", 10000)
                                .endObject()
                ).get();
        System.out.println(response.getResult());
    }

    private static void queryEmployee(TransportClient client) throws IOException {
        GetResponse response = client.prepareGet("company", "employee", "1").get();
        System.out.println(response.getSourceAsString());
    }

    private static void updateEmployee(TransportClient client) throws IOException {
        UpdateResponse response = client.prepareUpdate("company", "employee", "1")
                .setDoc(
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .field("position", "technique manager")
                                .endObject()
                ).get();
        System.out.println(response.getResult());
    }

    private static void deleteEmployee(TransportClient client) throws IOException{
        DeleteResponse response = client.prepareDelete("company","employee","1").get();
        System.out.println(response.getResult());
    }
}
