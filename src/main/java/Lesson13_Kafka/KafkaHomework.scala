package Lesson13_Kafka

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Duration
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._

object KafkaHomework extends App {

  Logger.getLogger("org").setLevel(Level.ERROR) // OFF
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getRootLogger.setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Kafka and Spark")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val inputData: DataFrame = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .option("nullValue", "")
    .csv("src/main/resources/kafka/bestsellers_with_categories.csv")

  inputData.printSchema()
  inputData.show(5, truncate = false)

  // Converting Row to JSON

  val inputJson: List[String] = inputData
    .toJSON
    .collect
    .toList

  // Kafka Admin part

  val config = new Properties()
  config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")

  var admin = AdminClient.create(config)

  // creating topics with partitions

  val topic = "book"
  val numPartitions = 3
  val bookTopic = new NewTopic(topic, numPartitions, 1.toShort)

  admin.createTopics(Collections.singleton(bookTopic))

  // Producer part

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092") // 29092 - inner docker kafka port
  props.put("parse.key", "true")

  val producer: KafkaProducer[String, String] = new KafkaProducer(
    props,
    new StringSerializer,
    new StringSerializer
  )

  inputJson
    .foreach { line =>
      producer.send(new ProducerRecord("book", line))
    }
  producer.close()

  // Consumer part

  val propsCons = new Properties()
  propsCons.put("bootstrap.servers", "localhost:29092") // 29092 - inner docker kafka port
  propsCons.put("group.id", "consumer1")

  val consumer = new KafkaConsumer(propsCons, new StringDeserializer, new StringDeserializer)

  def getRecordsByPartition(partitionNum: Int, recordsNum: Int): Iterable[ConsumerRecord[String, String]] = {
    consumer.assign(List(new TopicPartition(topic, partitionNum)).asJava)
    consumer
      .poll(Duration.ofMillis(1500))
      .asScala
      .filter(record => {
        record.partition() == partitionNum
      })
      .toArray
      .sortBy(_.offset())
      .take(recordsNum)
    }

  for (partitionNum <- 0 to 2) {
    getRecordsByPartition(partitionNum, 5)
      .foreach(record =>
        println(s"value: ${record.value()} where partition is ${record.partition()} and offset is ${record.offset()}"))
  }

  consumer.close()
}


