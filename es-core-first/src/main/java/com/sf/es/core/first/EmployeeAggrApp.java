package com.sf.es.core.first;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by xuery on 2018/4/8.
 */
public class EmployeeAggrApp {

    public static void main(String[] args) throws Exception {

        Settings settings = Settings.builder()
                .put("cluster.name", "my-es")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        SearchResponse searchResponse = client.prepareSearch("company")
                .setTypes("staff")
                .addAggregation(AggregationBuilders.terms("group_by_country").field("country")
                        .subAggregation(AggregationBuilders
                                .dateHistogram("group_by_join_date")
                                .field("join_date")
                                .dateHistogramInterval(DateHistogramInterval.YEAR)
                                .subAggregation(AggregationBuilders.avg("avg_salary").field("salary"))
                        )
                ).execute().actionGet();

        Aggregations aggregations = searchResponse.getAggregations();
        Map<String, Aggregation> aggregationMap = aggregations.asMap();
        StringTerms groupByCountry = (StringTerms) aggregationMap.get("group_by_country");
        Iterator<Terms.Bucket> groupByCountryBucketIt = groupByCountry.getBuckets().iterator();
        while(groupByCountryBucketIt.hasNext()){
            Terms.Bucket groupByCountryBucket = groupByCountryBucketIt.next();
            System.out.println(groupByCountryBucket.getKey()+"::"+groupByCountryBucket.getDocCount()+"-----");

            Histogram queryByJoinDate  = (Histogram) groupByCountryBucket.getAggregations().asMap().get("group_by_join_date");
            Iterator<Histogram.Bucket> queryByJoinDateBucketIt = queryByJoinDate.getBuckets().iterator();
            while(queryByJoinDateBucketIt.hasNext()){
                Histogram.Bucket groupByJoinDateBucket = queryByJoinDateBucketIt.next();
                System.out.print("   "+groupByJoinDateBucket.getKey()+"::"+groupByJoinDateBucket.getDocCount());
                Avg avgSalary = (Avg) groupByJoinDateBucket.getAggregations().asMap().get("avg_salary");
                System.out.println("   avg price:"+avgSalary.getValue());
            }
        }
    }
}
