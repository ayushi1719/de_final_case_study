from google.cloud import pubsub_v1

# Replace with your Google Cloud project ID, topic name, and subscription name
project_id = 'your-project-id'
topic_name = 'your-topic-name'
subscription_name = 'your-subscription-name'

subscriber = pubsub_v1.SubscriberClient()
topic_path = subscriber.topic_path(project_id, topic_name)
subscription_path = subscriber.subscription_path(project_id, subscription_name)

subscription = subscriber.create_subscription(
    request={"name": subscription_path, "topic": topic_path}
)
print(f"Created subscription: {subscription.name}")
