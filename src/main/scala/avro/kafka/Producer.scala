package avro.kafka

import java.io.{ByteArrayOutputStream, ByteArrayInputStream, File, FileInputStream, BufferedInputStream, InputStream}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Properties
import org.apache.avro.Schema
import org.apache.avro.file.{DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DatumWriter, EncoderFactory}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import org.apache.spark.sql.SparkSession


/**
  * Created by BaSaMaDi on 05/11/2018.
  */


object Producer extends App {

  val sparkSession = new SparkSession.Builder().getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")


  var AvroInputPath = args(0)
  var KafkaBroker = args(1)
  var KafkaTopic = args(2)

  private val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBroker)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, Array[Byte]](props)

  println("Start " + Producer.getClass)


  def AvroPublishKafka(avroDirPath: String) : Unit = {



    val datum: GenericDatumReader[GenericRecord] = new GenericDatumReader[GenericRecord]()


    def Serializer(record: GenericRecord) : Array[Byte]  = {

      val dataFileWriter : DataFileWriter[GenericRecord] = new DataFileWriter[GenericRecord](new GenericDatumWriter())
      val out : ByteArrayOutputStream = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(out, null)

      dataFileWriter.create(record.getSchema, out)
      dataFileWriter.append(record)

      encoder.flush()
      dataFileWriter.flush()
      out.toByteArray
    }

    def Deserializer(avroMessage: Array[Byte]) : Unit  = {

      val importReader: DataFileStream[GenericRecord] = new DataFileStream[GenericRecord](new ByteArrayInputStream(avroMessage), datum)
      val schema: Schema = importReader.getSchema
      while (importReader.hasNext) {
        val record: GenericRecord = importReader.next()

      }
    }
    def getListOfFiles(dir: String):List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }

    try {

      val avroFileList = getListOfFiles(avroDirPath).iterator

      while (avroFileList.hasNext) {

        var eventCount = 0

        val initialFile: File = avroFileList.next()
        val targetStream: InputStream = new FileInputStream(initialFile)
        val inputReader: DataFileStream[GenericRecord] = new DataFileStream[GenericRecord](new BufferedInputStream(targetStream), datum)
        val schema: Schema = inputReader.getSchema

        val datumWriter: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord]()

        while (inputReader.hasNext) {

          val record: GenericRecord = inputReader.next()
          try {
            val avroMessage = new ProducerRecord[String, Array[Byte]](KafkaTopic, null, Serializer(record))
            producer.send(avroMessage)
          }
          catch {
            case e: Exception => println("Error when publishing message")
          }
          eventCount += 1
        }
        val ingestionDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val ingestionDate = ingestionDateFormat.format(new Date())

        inputReader.close()
        println("{ingestionDate:"+ingestionDate+",avroFile:"+initialFile.getName+",fileSize:"+initialFile.length()+",eventCount:"+eventCount+"}")


      }
    } catch {
      case e: Exception => e.printStackTrace()
    }

    }

  AvroPublishKafka(AvroInputPath)

  println("End " + Producer.getClass)


}
