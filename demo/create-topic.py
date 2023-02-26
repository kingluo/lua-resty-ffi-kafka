from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="127.0.0.1:9092",
    client_id='test'
)

topic_list = []
topic_list.append(NewTopic(name="demo", num_partitions=1, replication_factor=1))
admin_client.create_topics(new_topics=topic_list, validate_only=False)
