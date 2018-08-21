package org.apache.rocketmq.store;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.rocketmq.store.config.BrokerRole.SLAVE;


public class CachedMessageStore implements MessageStore {


    private final MessageStoreConfig messageStoreConfig;

    /**
     * topic->queueId->message
     */
    private Map<String, Map<Integer, List<Integer>>> topic2queue2message;

    List<MessageExt> commitLog;

    CachedMessageStore(final MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
    }

    /**
     * @throws Exception
     */
    public void start() throws Exception {
        topic2queue2message = new ConcurrentHashMap<>();
        commitLog = Collections.synchronizedList(new ArrayList<MessageExt>());
    }

    /**
     *
     */
    public void shutdown() {

    }

    /**
     * @param msg
     * @return
     */
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        if (null == msg) {
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }
        return handleMessage(msg);
    }

    /**
     * @param messageExtBatch
     * @return
     */
    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        if (null == messageExtBatch) {
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }
        return handleMessage(messageExtBatch);
    }

    private PutMessageResult handleMessage(MessageExt messageExt) {
        boolean flag;
        if (null != topic2queue2message.get(messageExt.getTopic())) {
            flag = handleQueue(topic2queue2message.get(messageExt.getTopic()), messageExt);
        } else {
            Map<Integer, List<Integer>> queue2message = new ConcurrentHashMap<Integer, List<Integer>>(4);
            topic2queue2message.put(messageExt.getTopic(), queue2message);
            flag = handleQueue(topic2queue2message.get(messageExt.getTopic()), messageExt);
        }
        if (flag) {
            AppendMessageResult appendMessageResult = new AppendMessageResult(AppendMessageStatus.PUT_OK);
            return new PutMessageResult(PutMessageStatus.PUT_OK, appendMessageResult);
        } else {
            AppendMessageResult appendMessageResult = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, appendMessageResult);
        }
    }

    private boolean handleQueue(Map<Integer, List<Integer>> queue2message, MessageExt message) {
        if (null != queue2message.get(message.getQueueId())) {
            return handleMessage(queue2message.get(message.getQueueId()), message);
        } else {
            List<Integer> messageExtList = new LinkedList<Integer>();
            queue2message.put(message.getQueueId(), messageExtList);
            return handleMessage(messageExtList, message);
        }
    }

    private boolean handleMessage(List<Integer> messageExtList, MessageExt message) {
        commitLog.add(message);
        messageExtList.add(commitLog.size());
        return true;
    }


    /**
     * @param group
     * @param topic
     * @param queueId
     * @param offset
     * @param maxMsgNums
     * @param messageFilter
     * @return
     */
    public GetMessageResult getMessage(String group, String topic, int queueId, long offset, int maxMsgNums, MessageFilter messageFilter) {
        GetMessageResult result = new GetMessageResult();

        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;

        long nextBeginOffset = offset;
        long minOffset = 0;
        long maxOffset = 0;

        Map<Integer, List<Integer>> queues = topic2queue2message.get(topic);
        List<Integer> msgs = null;
        if (queues!=null) {
            msgs = queues.get(queueId);
        }

        if (msgs != null) {
            minOffset = msgs.get(0);
            maxOffset = msgs.get(msgs.size());
            if (maxOffset == 0) {
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = 0;
            } else if (offset < minOffset) {
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                nextBeginOffset = minOffset;
            } else if (offset == maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                nextBeginOffset = offset;
            } else if (offset > maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                if (0 == minOffset) {
                    nextBeginOffset = minOffset;
                } else {
                    nextBeginOffset = maxOffset;
                }
            } else {
                Integer index = msgs.indexOf(offset);
                if (index != -1)
                {
                    nextBeginOffset = msgs.get(index+1);
                } else {
                    status = GetMessageStatus.OFFSET_FOUND_NULL;
                    nextBeginOffset = minOffset;
                }
            }
        } else {
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = 0;
        }

        result.setStatus(status);
        result.setNextBeginOffset(nextBeginOffset);
        result.setMaxOffset(maxOffset);
        result.setMinOffset(minOffset);

        return result;
    }

    /**
     * @param topic
     * @param queueId
     * @return
     */
    public long getMaxOffsetInQueue(String topic, int queueId) {
        Map<Integer, List<Integer>> topicList = topic2queue2message.get(topic);
        if (null == topicList) {
            return 0;
        }
        List<Integer> queueList = topicList.get(queueId);
        if (null == queueList) {
            return 0;
        }
        return queueList.size();
    }

    public long getMinOffsetInQueue(String topic, int queueId) {
        Map<Integer, List<Integer>> topicList = topic2queue2message.get(topic);
        if (null == topicList) {
            return 0;
        }
        List<Integer> queueList = topicList.get(queueId);
        if (null == queueList) {
            return -1;
        }
        return queueList.size();
    }

    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        int len = 0;
        Map<Integer, List<Integer>> queueList = topic2queue2message.get(topic);
        if (null != queueList) {
            List<Integer> messageExtList = queueList.get(consumeQueueOffset);
            if (null != messageExtList) {
                for (Integer messageExt : messageExtList) {
                    len += messageExt;
                }
            }
        }
        return len;
    }

    public MessageExt lookMessageByOffset(long commitLogOffset) {
        if (commitLogOffset > Integer.MAX_VALUE) {
            return null;
        }
        MessageExt messageExt = commitLog.get((int)commitLogOffset);
        if (null != messageExt) {
            return  messageExt;
        }
        return null;
    }

    /**
     * @param offset
     * @return
     */
    public SelectMappedBufferResult getCommitLogData(long offset) {
        return null;
    }


    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        return null;
    }

    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return null;
    }

    public String getRunningDataInfo() {
        return null;
    }

    public HashMap<String, String> getRuntimeInfo() {
        return null;
    }

    public long getMaxPhyOffset() {
        return 0;
    }

    public long getMinPhyOffset() {
        return 0;
    }

    public long getEarliestMessageTime(String topic, int queueId) {
        return 0;
    }

    public long getEarliestMessageTime() {
        return 0;
    }

    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        return 0;
    }

    public long getMessageTotalInQueue(String topic, int queueId) {
        return 0;
    }

    public boolean appendToCommitLog(long startOffset, byte[] data) {
        return false;
    }

    public void executeDeleteFilesManually() {

    }

    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        return null;
    }

    public void updateHaMasterAddress(String newAddr) {

    }

    public long slaveFallBehindMuch() {
        return 0;
    }

    public long now() {
        return 0;
    }

    public int cleanUnusedTopic(Set<String> topics) {
        return 0;
    }

    public void cleanExpiredConsumerQueue() {

    }

    public boolean checkInDiskByConsumeOffset(String topic, int queueId, long consumeOffset) {
        return false;
    }

    public long dispatchBehindBytes() {
        return 0;
    }

    public long flush() {
        return 0;
    }

    public boolean resetWriteOffset(long phyOffset) {
        return false;
    }

    public long getConfirmOffset() {
        return 0;
    }

    public void setConfirmOffset(long phyOffset) {

    }

    public boolean isOSPageCacheBusy() {
        return false;
    }

    public long lockTimeMills() {
        return 0;
    }

    public boolean isTransientStorePoolDeficient() {
        return false;
    }

    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return null;
    }

    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        return null;
    }

    public boolean load() {
        return false;
    }


    public void destroy() {

    }

    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        return 0;
    }
}
