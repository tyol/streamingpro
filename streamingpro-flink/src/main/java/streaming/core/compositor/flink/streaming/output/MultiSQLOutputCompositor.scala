package streaming.core.compositor.flink.streaming.output

import java.util
import java.util.Properties

import org.apache.flink.table.api.Table
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sinks.{ConsoleTableSink, CsvTableSink}
import org.apache.flink.table.sinks.PrintlnSink
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer010,Kafka010JsonTableSink}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.log4j.Logger
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.flink.streaming.CompositorHelper
import streaming.core.strategy.ParamsValidator

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 20/3/2017.
  */
class MultiSQLOutputCompositor[T] extends Compositor[T] with CompositorHelper with ParamsValidator {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[MultiSQLOutputCompositor[T]].getName)


  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val tableEnv = params.get("tableEnv").asInstanceOf[TableEnvironment]
    _configParams.foreach { config =>

      val name = config.getOrElse("name", "").toString
      val _cfg = config.map(f => (f._1.toString, f._2.toString)).map { f =>
        (f._1, params.getOrElse(s"streaming.sql.out.${name}.${f._1}", f._2).toString)
      }.toMap

      val tableName = _cfg("inputTableName")
      val options = _cfg - "path" - "mode" - "format"
      val _resource = _cfg.getOrElse("path","-")
      val mode = _cfg.getOrElse("mode", "ErrorIfExists")
      val format = _cfg("format")
      val showNum = _cfg.getOrElse("showNum", "100").toInt


      val ste = tableEnv.asInstanceOf[StreamTableEnvironment]
      format match {
        case "csv" | "com.databricks.spark.csv" =>
          val csvTableSink = new CsvTableSink(_resource)
          //ste.ingest(tableName).writeToSink(csvTableSink)
          val projTable: Table = ste.scan(tableName)
          ste.registerTable("projectedTable", projTable)
          projTable.writeToSink(csvTableSink)
        case "console" | "print" =>
          //ste.ingest(tableName).writeToSink(new ConsoleTableSink(showNum))
          val projTable: Table = ste.scan(tableName)
          ste.registerTable("projectedTable", projTable)
          projTable.writeToSink(new ConsoleTableSink(showNum))
          //projTable.writeToSink(new PrintlnSink)
        case "kafka" =>
          //val outputTopic = _cfg("topic")
          //val kafkaBroker = _cfg("bootstrap.server")
          //throw new IllegalArgumentException
          val outputTopic = "yyj-output1"
          val kafkaBroker = "c1:6667,c2:6667,c3:6667"
          val properties = new Properties()
          properties.setProperty("bootstrap.servers",kafkaBroker)
          val proTable: Table = ste.scan(tableName)
          proTable.writeToSink(new Kafka010JsonTableSink(outputTopic,properties))
        case _ =>
      }
    }

    new util.ArrayList[T]()
  }

  override def valid(params: util.Map[Any, Any]): (Boolean, String) = {
    (true, "")
  }
}


