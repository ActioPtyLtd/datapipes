package actio.datapipes.dataSources

import java.io.ByteArrayOutputStream

import actio.common.Data.{DataNothing, DataSet, DataString}
import actio.common.{DataSource, Observer}

import scala.collection.mutable.ListBuffer
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.typesafe.scalalogging.Logger

import scala.collection.JavaConversions._

class AWSS3DataSource extends DataSource {
  private val logger = Logger("LocalFileSystemDataSource")
  private val _observer: ListBuffer[Observer[DataSet]] = ListBuffer()

  def subscribe(observer: Observer[DataSet]): Unit = _observer.append(observer)

  def execute(config: DataSet, query: DataSet*): Unit = {

    val yourAWSCredentials = new BasicAWSCredentials(
      config("credentials")("accessKey").stringOption.getOrElse(""),
      config("credentials")("accessSecret").stringOption.getOrElse(""))

    logger.info("Connecting to Amazon S3...")

    val changeEndpoint = (b: AmazonS3ClientBuilder) =>
    config("uri").stringOption.map(u =>
      b
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(u,config("region").stringOption.getOrElse("")))
        .withPathStyleAccessEnabled(true))
      .getOrElse(b.withRegion(config("region").stringOption.getOrElse("")))

    val amazonS3Client = changeEndpoint(AmazonS3ClientBuilder
      .standard)
      .withCredentials(new AWSStaticCredentialsProvider(yourAWSCredentials))
      .build

    logger.info("Successfully connected to Amazon S3.")

    val stream = new ByteArrayOutputStream()
    FileDataSource.writeData(stream, config("behavior").stringOption.getOrElse(""),config("compression").stringOption, query)
    val output = stream.toString

    val bucketName = config("bucketName").stringOption.getOrElse("")

    if(!amazonS3Client.doesBucketExist(bucketName))
      amazonS3Client.createBucket(bucketName)

    query.foreach { q =>
      amazonS3Client.putObject(bucketName, q("key").stringOption.getOrElse(""), output)
    }
  }
}
