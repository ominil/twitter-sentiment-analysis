# Twitter Streamer created 29 September 2022
import tweepy
from kafka import KafkaProducer
from kafka.errors import TopicAlreadyExistsError
from kafka.admin import KafkaAdminClient, NewTopic
from json import dumps

import os

def create_topic(topic_name=None, num_partitions=1, replication_factor=1):
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:9092", 
            client_id='test'
            )

        topic_list = []
        topic_list.append(NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        return True
    except TopicAlreadyExistsError as err:
        print(f"Request for topic creation is failed as {topic_name} is already created due to {err}")
        return False
    except Exception as err:
        print(f"Request for topic creation is failing due to {err}")
        return False

def init_kafka():
    return KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

class TweetStreaming(tweepy.StreamingClient):

    producer = init_kafka()

    def on_connect(self):
        print('Streaming intialized')


    def on_tweet(self, tweet):
        tweet = {'author': tweet.id, 'text':tweet.text}
        self.producer.send('tweets', value=tweet)
        


def stream_client(auth):
    return  TweetStreaming(auth)

def get_api_token():
    env_var = 'TWITTER_API_KEY'
    try:
        return os.environ[env_var]
    except KeyError:
        print(f'Environment variable {env_var} not set!')


def main():

    
    bearer_token = get_api_token()
                
    stream = stream_client(bearer_token)

    create_topic('tweets')

    # TODO better rules
    stream.add_rules(tweepy.StreamRule('Iphone lang:en'))
    stream.filter()
    
if __name__ == '__main__':
    main()
