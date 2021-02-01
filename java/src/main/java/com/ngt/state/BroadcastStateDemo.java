package com.ngt.state;

/**
 * @author ngt
 * @create 2021-02-01 12:02
 *  * 用户id，活动id，事件类型(1浏览，2参与)
 *  * user1,A,1
 *  * User1,A,1
 *  * User1,A,2
 *  * User2,A,1
 *  * User2,A,2
 *  * User3,A,2
 *  * User1,B,1
 *  * User1,B,2
 *  * User2,B,1
 *  * User3,A,1
 *  * User3,A,1
 *  * User3,B,1
 *  * User4,A,1
 *  * User4,A,1
 *  * 统计各个活动，事件的人数和次数
 *
 *  使用 BroadcastState 将事实表和维度表进行连接操作
 *  https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/stream/state/broadcast_state.html
 */
public class BroadcastStateDemo {
    public static void main(String[] args) {

    }
}
