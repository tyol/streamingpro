package streaming.core.compositor.flink.streaming.source

import java.util
import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010,Kafka010JsonTableSource}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.{TableEnvironment,TableSchema}
import org.apache.log4j.Logger
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.flink.streaming.CompositorHelper
import streaming.core.strategy.platform.FlinkStreamingRuntime
import org.apache.flink.table.api.scala._

import org.apache.flink.table.eventtime.LinearTimestamp
import scala.collection.JavaConversions._


class MultiStreamingCompositor[T] extends Compositor[T] with CompositorHelper {

  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[MultiStreamingCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  private def getKafkaParams(params: util.Map[Any, Any]) = {
    params.filter {
      f =>
        if (f._1 == "topics") false else true
    }.toMap.asInstanceOf[Map[String, String]]
  }


  private def getZk(params: util.Map[Any, Any]) = {
    getKafkaParams(params).getOrElse("zk", getKafkaParams(params).getOrElse("zookeeper", getKafkaParams(params).getOrElse("zkQuorum", "127.0.0.1")))
  }
  private def getKB(params: util.Map[Any, Any]) = {
    getKafkaParams(params).getOrElse("bootstrap.servers", getKafkaParams(params).getOrElse("kafka.brokers", getKafkaParams(params).getOrElse("kafka", "127.0.0.1")))
  }
  private def getTopics(params: util.Map[Any, Any]) = {
    params.get("topics").asInstanceOf[String].split(",").toSet
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val runtime = params.get("_runtime_").asInstanceOf[FlinkStreamingRuntime]
    val env = runtime.runtime
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    params.put("tableEnv",tableEnv)

    val kafkaListStream = _configParams.map { p =>

      p.getOrElse("format","-") match {
        case "kafka" =>
          val topics = getTopics(p).mkString(",")
          val kafkaBroker = getKB(p)
          val properties = new Properties()
          properties.setProperty("bootstrap.servers",kafkaBroker)
          implicit val typeInfo = TypeInformation.of(classOf[String])
          val kafkaStreamtmp = env.addSource(new FlinkKafkaConsumer010[String](topics, new SimpleStringSchema(), properties))
          val kafkaStream = kafkaStreamtmp.assignTimestampsAndWatermarks(new LinearTimestamp)
          tableEnv.registerDataStream(p("outputTable").toString, kafkaStream,'f0,'userActionTime.rowtime)
          kafkaStream
        case "kafka.json" =>
          val topics = getTopics(p).mkString(",")
          val kafkaBroker = getKB(p)
          val properties = new Properties()
          properties.setProperty("bootstrap.servers",kafkaBroker)
          implicit val typeInfo = TypeInformation.of(classOf[String])
          val kafkaStream = Kafka010JsonTableSource.builder().forTopic(topics).withKafkaProperties(properties).withSchema(TableSchema.builder()
                            .field("name1",Types.STRING)
                            .field("name2",Types.STRING)
                            .field("name3",Types.STRING).build()).build()
          tableEnv.registerTableSource(p("outputTable").toString,kafkaStream)
          kafkaStream
        case "socket" =>
          val socketStreamtmp = env.socketTextStream(
            p.getOrElse("host","localhost").toString,
            p.getOrElse("port","9000").toString.toInt,
            '\n')
          val socketStream = socketStreamtmp.assignTimestampsAndWatermarks(new LinearTimestamp)
          tableEnv.registerDataStream(p("outputTable").toString, socketStream,'f0,'userActionTime.rowtime)
          socketStream
        case _ =>
      }


    }

    List(kafkaListStream.asInstanceOf[T])
  }

}
