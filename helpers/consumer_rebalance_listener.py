from kafka import ConsumerRebalanceListener, TopicPartition, OffsetAndMetadata

current_offset = {}

class ConsumerRebalanceListenerHandler(ConsumerRebalanceListener):
    def __init__(self, consumer):
        self.consumer = consumer 

    def get_current_offset(self):
        return current_offset

    def add_offset(self, topic, partition, offset):
        key = TopicPartition(topic, partition)
        current_offset[key] = OffsetAndMetadata(offset=offset, metadata='commit', leader_epoch=0)

    def on_partitions_revoked(self, revoked):
        print(f"Partitions revoked: {revoked}")
        self.consumer.commit(self.get_current_offset())
        current_offset.clear()

    def on_partitions_assigned(self, assigned):
        print(f"Partitions assigned: {assigned}")
