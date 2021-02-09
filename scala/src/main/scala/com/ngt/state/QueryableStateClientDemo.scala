package com.ngt.state

import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.queryablestate.client.QueryableStateClient

import java.util.concurrent.{CompletableFuture, TimeUnit}

/**
 * @author ngt
 * @create 2021-02-09 8:09
 */
object QueryableStateClientDemo {
  def main(args: Array[String]): Unit = {
    val client: QueryableStateClient = new QueryableStateClient("localhost", 9069)
    val stateDescriptor: ValueStateDescriptor[Int] = new ValueStateDescriptor[Int]("wc-state", classOf[Int])
    val future: CompletableFuture[ValueState[Int]] = client.getKvState(
      // 注意JobID每次都不一样
      JobID.fromHexString("31081677fbc9fe700ca7d5c881abc709"),
      "my-query-name",
      "flink",
      BasicTypeInfo.STRING_TYPE_INFO,
      stateDescriptor
    )

    future.thenAccept(response => {
      try {
        val value = response.value();
        System.out.println(value);
      } catch {
        case ex: Exception => ex.printStackTrace()
      }
    })

    TimeUnit.SECONDS.sleep(5)
  }
}
