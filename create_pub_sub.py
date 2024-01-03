from google.cloud import pubsub_v1

# Replace with your Google Cloud project ID and topic name
project_id = 'your-project-id'
topic_name = 'your-topic-name'

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

topic = publisher.create_topic(request={"name": topic_path})
print(f"Created topic: {topic.name}")