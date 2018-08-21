package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.*;

public class OrderedMQPullConsumer extends DefaultMQPullConsumer {
    /**
     * 每个队列每次拉取的条数
     */
    private static final int EACH_QUEUE_PULL_NUM = 10;

    private int eachQueuePullNum = EACH_QUEUE_PULL_NUM;

    public OrderedMQPullConsumer() {
    }

    public OrderedMQPullConsumer(String consumerGroup, RPCHook rpcHook) {
        super(consumerGroup, rpcHook);
    }

    public OrderedMQPullConsumer(String consumerGroup) {
        super(consumerGroup);
    }

    public OrderedMQPullConsumer(RPCHook rpcHook) {
        super(rpcHook);
    }

    /**
     * 设置每个队列每次拉取的条数
     *
     * @param eachQueuePullNum 条数
     */
    public void setEachQueuePullNum(int eachQueuePullNum) {
        this.eachQueuePullNum = eachQueuePullNum;
    }

    /**
     * 拉取消息
     *
     * @param topic         话题
     * @param subExpression 子查询表达式
     * @param maxNums       最大数量
     * @return 拉取结果
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    public PullResult pull(String topic, String subExpression, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        //取出话题下的所有队列
        Set<MessageQueue> messageQueues = fetchSubscribeMessageQueues(topic);
        HashMap<Integer, Long> offsetMap = new HashMap<>(messageQueues.size());
        //包装类的有序集合，按照每个队列的第一条消息进行排序
        TreeSet<Wrapper> sortedWrappers = new TreeSet<>();
        for (MessageQueue mq : messageQueues) {
            //遍历消息队列，把每个队列的偏移量取出来，并且按照偏移量去取一次消息，封装成包装类添加到包装类的队列中
            offsetMap.put(mq.getQueueId(), fetchConsumeOffset(mq, true));
            Wrapper wrapper = pullByQueue(mq, subExpression, offsetMap, EACH_QUEUE_PULL_NUM);
            if (wrapper != null) {
                sortedWrappers.add(wrapper);
            }
        }
        //消息结果列表
        List<MessageExt> resultMessages = new ArrayList<>();
        while (resultMessages.size() < maxNums) {
            //从队列中取出一个包装队列
            Wrapper wrapper = sortedWrappers.pollFirst();
            if (wrapper == null) {
                break;
            }
            //从包装队列中取出一条消息，再按照新的队首消息进行排序
            MessageExt messageExt = wrapper.poll();
            if (messageExt == null) {
                //如果当前队列已为空，则再去拉取消息
                wrapper = pullByQueue(wrapper.messageQueue, subExpression, offsetMap, EACH_QUEUE_PULL_NUM);
                if (wrapper != null) {
                    sortedWrappers.add(wrapper);
                }
            } else {
                resultMessages.add(messageExt);
                sortedWrappers.add(wrapper);
            }
        }
        //返回最终的结果，这里 offset 暂时没有进行处理
        if (resultMessages.isEmpty()) {
            return new PullResult(PullStatus.NO_NEW_MSG, 0, 0, 0, resultMessages);
        }
        return new PullResult(PullStatus.FOUND, 0, 0, 0, resultMessages);
    }

    /**
     * 从队列中拉取一次消息
     *
     * @param messageQueue  消息队列
     * @param subExpression 子查询表达式
     * @param offsetMap     每个队列的偏移量
     * @param num           拉取数目
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    private Wrapper pullByQueue(MessageQueue messageQueue, String subExpression, HashMap<Integer, Long> offsetMap,
                                int num)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        Long offset = offsetMap.remove(messageQueue.getQueueId());
        if (offset == null) {
            return null;
        }
        PullResult pullResult = pull(messageQueue, subExpression, offset, num);
        if (pullResult.getPullStatus() != PullStatus.FOUND) {
            return null;
        }
        offsetMap.put(messageQueue.getQueueId(), pullResult.getNextBeginOffset());
        return new Wrapper(messageQueue, new LinkedList<>(pullResult.getMsgFoundList()));
    }

    /**
     * 队列的包装类
     */
    static class Wrapper implements Comparable<Wrapper> {
        /**
         * 消息队列对象
         */
        MessageQueue messageQueue;
        /**
         * 消息列表
         */
        LinkedList<MessageExt> messageList;

        Wrapper(MessageQueue messageQueue, LinkedList<MessageExt> messageList) {
            this.messageQueue = messageQueue;
            this.messageList = messageList;
        }

        /**
         * 取出一条消息
         *
         * @return 消息
         */
        MessageExt poll() {
            return messageList.poll();
        }

        /**
         * 用于队列的比较方法，按照每个队列的第一条消息进行排序
         *
         * @param that 另一个队列的包装类
         * @return 大小值
         */
        @Override
        public int compareTo(Wrapper that) {
            if (this.messageList.isEmpty()) {
                return that.messageList.isEmpty() ? 0 : 1;
            }
            return (int) (this.messageList.get(0).getBornTimestamp() - that.messageList.get(0).getBornTimestamp());
        }
    }
}
