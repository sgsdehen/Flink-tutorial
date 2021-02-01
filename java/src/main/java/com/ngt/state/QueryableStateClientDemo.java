package com.ngt.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author ngt
 * @create 2021-02-01 19:37
 */
public class QueryableStateClientDemo {
    public static void main(String[] args) throws Exception {
        // 此处本地地址必须要写localhost，不能写成 ip 的形式
        QueryableStateClient client = new QueryableStateClient("localhost", 9069);

        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("wc-state", Integer.class);
        // 启用状态查询

        CompletableFuture<ValueState<Integer>> future = client.getKvState(
                JobID.fromHexString("31081677fbc9fe700ca7d5c881abc709"),  // jobId
                "my-query-name",
                "flink",
                BasicTypeInfo.STRING_TYPE_INFO,
                stateDescriptor);

        future.thenAccept(response -> {
            try {
                Integer value = response.value();
                System.out.println(value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });


        TimeUnit.SECONDS.sleep(5);
    }
}
