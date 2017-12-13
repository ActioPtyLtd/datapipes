package actio.datapipes.dataSources

import java.io._
import java.lang.Exception
import java.net.URL

import actio.common.Data.DataSet
import actio.common.{DataSource, Observer}
import io.tus.java.client._
import java.util.zip._

import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import java.util.zip.GZIPOutputStream

import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import com.typesafe.scalalogging.Logger
import org.apache.commons.compress.utils.IOUtils
import io.tus.java.client.TusUploader

import scala.io.Source

class TusDataSource extends DataSource {
  private val logger = Logger("TusDataSource")

  def execute(config: DataSet, query: DataSet*): Unit = {

    val uri = query.head("uri").stringOption.getOrElse("")
    val source = config("directory").stringOption.getOrElse("")
    val folder = new java.io.File(source)
    val filePaths = Option(folder.listFiles)

    logger.info(s"Path to be uploaded: ${source}")

    if(filePaths.isDefined && filePaths.get.size > 0) {

      logger.info(s"${filePaths.get.size} files found. Compressing files...")

      val fos = new FileOutputStream(source.concat(".tar.gz"))
      val tarOs = new TarArchiveOutputStream(fos)

      for (file <- filePaths.get) {
        if (file.isFile) {
          val fis = new FileInputStream(file)
          val bis = new BufferedInputStream(fis)
          // Write content of the file
          tarOs.putArchiveEntry(new TarArchiveEntry(file, file.getName))
          IOUtils.copy(bis, tarOs)
          tarOs.closeArchiveEntry()
          fis.close()
        }
      }

      tarOs.close()

      logger.info(s"File ${source.concat(".tar.gz")} successfully generated.")
    } else {
      logger.warn(s"No files found at path: ${source}")
    }

    val delays = config("delays").elems.flatMap(_.intOption).toArray

    // list could be null
    val tars = Option(folder.getParentFile.listFiles())
      .toList
      .flatten
      .filter(f => f.getName.endsWith(".tar.gz"))
      .sortBy(s => s.lastModified())

    logger.info(s"${tars.size} tars found. ")

    if(tars.nonEmpty)
      logger.info("Iterating through tar files and sync...")

    tars.foreach { t=>
      upload_uri(t.getPath, uri, delays, true)
    }

  }


  def upload_uri(source:String, uri: String, delays: Array[Int], deleteAfter: Boolean): Unit = {
    val client = new TusClient()
    val store = new TusLocalFileStore

    client.setUploadCreationURL(new URL(uri))

    import collection.JavaConversions._
    val addActioKey = Map("actio_key" -> "hello")

    client.setHeaders(addActioKey)
    client.enableResuming(store)

    val file = new File(source)

    val upload = new TusUpload(file)

    val executor = new TusExecutor() {
      override def makeAttempt(): Unit = {
        logger.info(s"Connecting to sync server: ${uri}")
        val uploader: TusUploader = client.resumeOrCreateUpload(upload)
        logger.info("Successfully connected to sync server.")
        logger.info(s"Upload fingerprint: ${upload.getFingerprint}")
        logger.info(s"Upload URL: ${uploader.getUploadURL.toString}")

        uploader.setChunkSize(1024)

        try {
          do {
            val totalBytes = upload.getSize
            val bytesUploaded = uploader.getOffset
            val chunkSize = uploader.getChunkSize
            val progress: Double = bytesUploaded.toDouble / totalBytes * 100

            if ((bytesUploaded.toDouble / totalBytes * 10).toInt - ((bytesUploaded.toDouble - chunkSize) / totalBytes * 10).toInt == 1) {
              logger.info(s"${progress.toInt}% uploaded.")
            }

          } while (uploader.uploadChunk() > -1)
        }
        catch {
          case (e: Exception) => {
            logger.info(e.toString)
            throw e
          }
        }

        uploader.finish()
        logger.info("Upload completed successfully.")

        if(deleteAfter) {
          new File(source).delete()
          store.remove(upload.getFingerprint)
        }
      }
    }

    if(delays.nonEmpty)
      executor.setDelays(delays)

    executor.makeAttempts()
  }

  def subscribe(observer: Observer[DataSet]): Unit = {

  }
}


class TusLocalFileStore extends TusURLStore {

  override def set(s: String, url: URL): Unit = {
    val writer = new PrintWriter(new File(fileHash(s)))
    writer.write(url.toString)
    writer.close()
  }

  override def remove(s: String): Unit = {
    new File(fileHash(s)).delete()
  }

  override def get(s: String): URL = {
    if (new File(fileHash(s)).exists())
      new URL(Source.fromFile(fileHash(s)).mkString)
    else
      null
  }

  def fileHash = (s: String) => {
    "t-" + s.hashCode.toString + ".tus"
  }
}