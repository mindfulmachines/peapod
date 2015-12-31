package dependency

import java.io.{ObjectInputStream, ObjectOutputStream, ByteArrayOutputStream}
import java.net.URI

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

object StorableTask {

  private def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  private def saveAsCompressedObjectFile(rdd: RDD[_], path: String): Unit = {
    rdd.mapPartitions(iter => iter.grouped(10).map(_.toArray))
      .map(x => (NullWritable.get(), new BytesWritable(StorableTask.serialize(x))))
      .saveAsSequenceFile(path, Some(classOf[BZip2Codec]))
  }

  implicit class DataFrameStorable[V <: DataFrame](df: V) extends Storable[V] {
    def readStorable(path: String): V = {

    }
    def writeStorable(path: String) = {

    }
  }

  implicit class RDDStorable[V <: RDD[_]](df: V) extends Storable[V] {
    def readStorable(path: String): V = {

    }
    def writeStorable(path: String) = {

    }
  }

  implicit class SerializableStorable[V <: Serializable](df: V) extends Storable[V] {
    def readStorable(path: String): V = {

    }
    def writeStorable(path: String) = {

    }
  }

}

trait Storable[V] {
  def readStorable(path: String): V
  def writeStorable(path: String)
}

abstract class StorableTask[V <: Storable: ClassTag](implicit val p: Peapod)
  extends Task[V] with Logging {
  protected def generate: V

  protected[dependency] def build(): V = {
    logInfo("Loading" + dir)
    logInfo("Loading" + dir + " Exists: " + exists)
    val rdd = if(! exists()) {
      val rddGenerated = generate
      logInfo("Loading" + dir + " Deleting")
      delete()
      logInfo("Loading" + dir + " Generating")
      write(rddGenerated)
      rddGenerated
      read() match {
        case Some(v) => v
        case None =>
          logInfo("Loading" + dir + " Generating")
          rddGenerated
      }
    } else {
      logInfo("Loading" + dir + " Reading")
      read() match {
        case Some(v) => v
        case None =>
          logInfo("Loading" + dir + " Generating")
          generate
      }
    }
    if(shouldPersist()) {
      logInfo("Loading" + dir + " Persisting")
      persist(rdd)
    } else {
      rdd
    }
  }

  protected def read(): Option[V] = {
    if (classOf[RDD[_]].isAssignableFrom(implicitly[ClassTag[V]].runtimeClass)) {
      Some(
        p.sc.objectFile[V](dir, p.parallelism).asInstanceOf[V]
      )
    } else if (classOf[DataFrame].isAssignableFrom(implicitly[ClassTag[V]].runtimeClass)) {
      if(p.fs.startsWith("s3n")) {
        //There's a bug in the parquet reader for S3 so it doesn't properly get the hadoop configuration key and secret
        val awsKey = p.sc.hadoopConfiguration.get("fs.s3n.awsAccessKeyId")
        val awsSecret = p.sc.hadoopConfiguration.get("fs.s3n.awsSecretAccessKey")
        Some(
          p.sqlCtx.read.parquet(p.fs + awsKey + ":" + awsSecret + "@" + p.path + "/" + name + "/" + recursiveVersionShort).asInstanceOf[V]
        )
      } else {
        Some(
          p.sqlCtx.read.parquet(dir).asInstanceOf[V]
        )
      }
    } else if (
      classOf[Serializable].isAssignableFrom(implicitly[ClassTag[V]].runtimeClass)
    ) {
      val fs = FileSystem.get(new URI(dir), p.sc.hadoopConfiguration)
      val in = fs.open(new Path(dir + "/serialized.dat"))
      val objReader = new ObjectInputStream(in)
      val obj = objReader.readObject().asInstanceOf[V]
      in.close()
      fs.close()
      Some(obj.asInstanceOf[V])
    } else {
      logInfo("Loading" + dir + " Not Readable")
      None
    }
  }
  protected def write(v: V): Unit = {
    v match {
      case rdd: RDD[_] => StorableTask.saveAsCompressedObjectFile(rdd, dir)
      case df: DataFrame => df.write.parquet(dir)
      case s: Serializable =>
        val fs = FileSystem.get(new URI(dir), p.sc.hadoopConfiguration)
        val out = fs.create(new Path(dir + "/serialized.dat"))
        val objWriter = new ObjectOutputStream(out)
        objWriter.writeObject(s)
        fs.createNewFile(new Path(dir + "/_SUCCESS"))
        objWriter.close()
        fs.close()
      case _ => logInfo("Loading" + dir + " Not Writable")
    }
  }
  protected def persist(v: V): V = {
    v match {
      case rdd: RDD[_] => rdd.persist(StorageLevel.MEMORY_AND_DISK).asInstanceOf[V]
      case df: DataFrame => df.cache().asInstanceOf[V]
      case _ =>
        logInfo("Loading" + dir + " Not Persistable")
        v
    }
  }

  protected def delete() {
    val fs = FileSystem.get(new URI(dir), p.sc.hadoopConfiguration)
    fs.delete(new Path(dir), true)
  }
  def exists(): Boolean = {
    val fs = FileSystem.get(new URI(dir), p.sc.hadoopConfiguration)
    fs.isFile(new Path(dir + "/_SUCCESS"))
  }

}
