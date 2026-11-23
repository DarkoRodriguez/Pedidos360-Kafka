package pedidos360.analytics.dto;

public class ConsumerGroupInfoDto {

    private String groupId;
    private String state;
    private int membersCount;
    private boolean isSimpleConsumerGroup;
    private String partitionAssignor;

    public ConsumerGroupInfoDto() {}

    public ConsumerGroupInfoDto(String groupId, String state, int membersCount, boolean isSimpleConsumerGroup, String partitionAssignor) {
        this.groupId = groupId;
        this.state = state;
        this.membersCount = membersCount;
        this.isSimpleConsumerGroup = isSimpleConsumerGroup;
        this.partitionAssignor = partitionAssignor;
    }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }

    public String getState() { return state; }
    public void setState(String state) { this.state = state; }

    public int getMembersCount() { return membersCount; }
    public void setMembersCount(int membersCount) { this.membersCount = membersCount; }

    public boolean isSimpleConsumerGroup() { return isSimpleConsumerGroup; }
    public void setSimpleConsumerGroup(boolean simpleConsumerGroup) { isSimpleConsumerGroup = simpleConsumerGroup; }

    public String getPartitionAssignor() { return partitionAssignor; }
    public void setPartitionAssignor(String partitionAssignor) { this.partitionAssignor = partitionAssignor; }
}
