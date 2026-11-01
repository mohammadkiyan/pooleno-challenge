-- Both consumers now set unreadable messages aside here, so each row names its consumer.
ALTER TABLE external_quarantine RENAME TO quarantine;

ALTER TABLE quarantine ADD COLUMN consumer text NOT NULL DEFAULT 'external-notification-v1';
ALTER TABLE quarantine ALTER COLUMN consumer DROP DEFAULT;

DELETE FROM quarantine newer
 USING quarantine older
 WHERE newer.id > older.id
   AND newer.consumer = older.consumer
   AND newer.topic = older.topic
   AND newer.kafka_partition = older.kafka_partition
   AND newer.kafka_offset = older.kafka_offset
   AND newer.raw_value = older.raw_value;

-- A redelivery after a crash adds no second row. The hash is part of the key because Kafka's
-- offsets restart if its data is lost, and a different message at a reused position must
-- still be recorded. A hash because a value can exceed what an index entry can hold.
CREATE UNIQUE INDEX quarantine_message_key
  ON quarantine (consumer, topic, kafka_partition, kafka_offset, md5(raw_value));
