from confluent_kafka import Producer
from json import dumps as json_dumps
from logging import getLogger
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase


LOG = getLogger(__name__)


class DocManager(DocManagerBase):
    """ DocManager that echoes MongoDB Oplog to Kafka.
    """
    _topic_prefix = 'db.mongo.'

    def __init__(self, url, **kwargs):
        """ Sets up producer connection to Kafka.

        Parameters
        ----------
        url : str
            Directly corresponds to the "bootstrap.servers" config when initializing a Kafka entity
        """
        self.producer = Producer({'bootstrap.servers': url})

    def commit(self):
        self.producer.flush()

    def get_last_doc(self):
        """ TODO: For now, this returns nothing.
        """
        pass

    def remove(self, document_id, namespace, timestamp):
        """ Sends a remove message to the corresponding kafka topic.

        Parameters
        ----------
        document_id : str
        namespace : str
        timestamp : bson.timestamp.Timestamp
        """
        msg_topic = self._get_topic(namespace)
        msg_key = document_id
        msg_val = json_dumps({
            'op': 'remove',
            'o': document_id,
            'ts': timestamp,
        })

        return self._produce(msg_topic, msg_key, msg_val)

    def search(self, start_ts, end_ts):
        """ TODO: For now, this returns an empty iterator.
        """
        return iter([])

    def stop(self):
        self.producer.flush()

    def update(self, document_id, update_spec, namespace, timestamp):
        """ Sends an update message to the corresponding kafka topic.

        Parameters
        ----------
        document_id : str
        update_spec : dict
        namespace : str
        timestamp : bson.timestamp.Timestamp
        """
        msg_topic = self._get_topic(namespace)
        msg_key = document_id
        msg_val = json_dumps({
            'op': 'update',
            'o': update_spec,
            'o2': document_id,
            'ts': timestamp,
        })

        return self._produce(msg_topic, msg_key, msg_val)

    def upsert(self, document, namespace, timestamp):
        """ Sends an upsert message to the corresponding kafka topic.

        Parameters
        ----------
        document : dict
        namespace : str
        timestamp : bson.timestamp.Timestamp
        """
        msg_topic = self._get_topic(namespace)
        msg_key = document['_id']
        msg_val = json_dumps({
            'op': 'upsert',
            'o': document,
            'ts': timestamp,
        })

        return self._produce(msg_topic, msg_key, msg_val)

    def _produce(self, topic, key, value):
        """ Helper method for producing to Kafka.
        """
        return self.producer.produce(topic=topic, key=key, value=value,
                                     callback=self._delivery_report)

    @staticmethod
    def _get_topic(namespace):
        """ Returns a Kafka topic name based on given parameters.

        Parameters
        ----------
        namespace : str
        """
        return '{}.{}'.format(DocManager._topic_prefix, namespace)

    @staticmethod
    def _delivery_report(err, msg):
        if err is None:
            LOG.info('Message with key {} produced to topic {}: {}'
                     .format(msg.key(), msg.topic(), msg.value()))
        else:
            LOG.error('Error while delivering message with key {} to topic {}, with value {}:\n{}'
                      .format(msg.key(), msg.topic(), msg.value(), err.str()))
