

from kafka import KafkaConsumer
# class ConsumerRun:
#     def __init__(self, config):
#         self.cf = config
#         self.p = ProducerRun(self.cf.get("kfk_pd", "ser"), self.cf.get("log", "lg_e"), self.cf.get("kfk_pd", "client_id"))
#         self.tp = self.cf.get("kfk_pd", "tp_td")  # YOUR_OUTPUT_TOPIC
#         self.lg = self.cf.get("log", "lg_e")
#         self.con_tout = self.cf.get("sys", "con_tout")
#
#     def connect(self, kafka_servers, kafka_group, kafka_topics, client_id):
#         self.c = Consumer({'bootstrap.servers': kafka_servers, 'group.id': kafka_group, 'client.id': client_id})
#         self.c.subscribe(kafka_topics)
#
#     def data_handler(self, data):
#         pass
#
#     def run(self):
#         self.running = True
#         logger = logging.getLogger(self.lg)
#         while self.running:
#             msg = self.c.poll(self.con_tout)
#             if msg:  # poll加timeout参数，msg可能为空
#                 if not msg.error():
#                     logger.info('Received message:key=%s' % msg.key())
#                     try:
#                         self.data_handler(msg.value())
#                     except Exception as e:
#                         logger.error('Failed to handle data key=%s. Continue.' % msg.key(), exc_info=True)
#                         continue
#                 elif msg.error().code() != KafkaError._PARTITION_EOF:
#                     logger.error('Received error message:%s' % str(msg.error()))
#                 else:
#                     logger.error('No more message:%s' % str(msg.error()))
#         self.c.close()
#         self.running = False
#
#     def stop(self):
#         self.running = False
#
#
# if __name__ == "__main__":
#
#     c = ConsumerRun()