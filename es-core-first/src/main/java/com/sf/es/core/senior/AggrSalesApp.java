package com.sf.es.core.senior;

import com.sf.es.core.common.EsClientUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by xuery on 2018/5/3.
 */
public class AggrSalesApp {

    public static void main(String[] args) {
        TransportClient client = EsClientUtils.getTransportClient();

        try {
            SearchResponse searchResponse = client.prepareSearch("car_shop")
                    .setTypes("cars")
                    .addAggregation(AggregationBuilders.terms("group_by_brand").field("brand.raw")
                            .subAggregation(AggregationBuilders.dateHistogram("group_by_produce_date").field("produce_date")
                                    .dateHistogramInterval(DateHistogramInterval.MONTH)
                                    .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
                                    .subAggregation(AggregationBuilders.sum("sum_price").field("price"))
                            )
                    )
                    .get();

            Aggregations aggregations = searchResponse.getAggregations();

            Map<String, Aggregation> aggregationMap = aggregations.asMap();
            StringTerms stringTerms = (StringTerms) aggregationMap.get("group_by_brand");
            Iterator<Terms.Bucket> groupByBrandBucketIt = stringTerms.getBuckets().iterator();
            while (groupByBrandBucketIt.hasNext()) {
                Terms.Bucket groupByBrandBucket = groupByBrandBucketIt.next();
                System.out.println(groupByBrandBucket.getKey() + "::" + groupByBrandBucket.getDocCount());

                //第二层
                Histogram dateHistogram = (Histogram) groupByBrandBucket.getAggregations().asMap().get("group_by_produce_date");
                Iterator<Histogram.Bucket> dateHistogramBucketIt = dateHistogram.getBuckets().iterator();
                while (dateHistogramBucketIt.hasNext()) {
                    Histogram.Bucket dateHistogramBucket = dateHistogramBucketIt.next();
                    System.out.println("    " + dateHistogramBucket.getKey() + "::" + dateHistogramBucket.getDocCount());

                    //第三层
                    Avg avgPrice = (Avg) dateHistogramBucket.getAggregations().asMap().get("avg_price");
                    Sum sumPrice = (Sum) dateHistogramBucket.getAggregations().asMap().get("sum_price");
                    System.out.println("        avg_price::" + avgPrice.getValue());
                    System.out.println("        sum_price::" + sumPrice.getValue());
                }
            }
        } catch (Exception e){

        }finally {
            client.close();
        }
    }
}
