package dependency

import java.io.{ObjectInputStream, ObjectOutputStream, ByteArrayOutputStream}
import java.net.URI

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.hadoop.io._
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import scala.reflect._

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

  class DataFrameStorable(df: DataFrame) extends Storable[DataFrame] {
    def readStorable(p: Peapod, fs: String, path: String): DataFrame = {
      if(fs.startsWith("s3n")) {
        //There's a bug in the parquet reader for S3 so it doesn't properly get the hadoop configuration key and secret
        val awsKey = p.sc.hadoopConfiguration.get("fs.s3n.awsAccessKeyId")
        val awsSecret = p.sc.hadoopConfiguration.get("fs.s3n.awsSecretAccessKey")
          p.sqlCtx.read.parquet(fs + awsKey + ":" + awsSecret + "@" + path)
      } else {
          p.sqlCtx.read.parquet(fs + path)
      }
    }
    def writeStorable(p: Peapod, fs: String, path: String) = {
      df.write.parquet(fs + path)
    }
    override def persistStorable(): DataFrame = {
      df.cache()
    }
  }

  class RDDStorable[W: ClassTag](rdd: RDD[W]) extends Storable[RDD[W]] {
    def readStorable(p: Peapod, fs: String, path: String): RDD[W] = {
      p.sc.objectFile[W](fs + path)
    }
    def writeStorable(p: Peapod, fs: String, path: String) = {
      StorableTask.saveAsCompressedObjectFile(rdd, fs + path)
    }
    override def persistStorable(): RDD[W] = {
      rdd.persist(StorageLevel.MEMORY_AND_DISK)
    }
  }

  class SerializableStorable[V <: Serializable: ClassTag](s: V) extends Storable[V] {
    def readStorable(p: Peapod, fs: String, path: String): V = {
      val filesystem = FileSystem.get(new URI(fs + path), p.sc.hadoopConfiguration)
      val in = filesystem.open(new Path(fs + path + "/serialized.dat"))
      val objReader = new ObjectInputStream(in)
      val obj = objReader.readObject().asInstanceOf[V]
      in.close()
      filesystem.close()
      obj
    }
    def writeStorable(p: Peapod, fs: String, path: String) = {
      val filesystem = FileSystem.get(new URI(fs + path), p.sc.hadoopConfiguration)
      val out = filesystem.create(new Path(path + "/serialized.dat"))
      val objWriter = new ObjectOutputStream(out)
      objWriter.writeObject(s)
      objWriter.close()
      filesystem.close()
    }
    def persistStorable() = s
  }

  class WritableConvertedStorable[V : ClassTag, W <: Writable: ClassTag]
      (s: V, ctw: V => W, wtc: W => V) extends Storable[V] {
    def readStorable(p: Peapod, fs: String, path: String): V = {
      val filesystem = FileSystem.get(new URI(fs + path), p.sc.hadoopConfiguration)
      val in = filesystem.open(new Path(fs + path + "/serialized.dat"))
      val obj = classTag[W].runtimeClass.newInstance().asInstanceOf[W]
      obj.readFields(in)
      in.close()
      filesystem.close()
      wtc(obj)
    }
    def writeStorable(p: Peapod, fs: String, path: String) = {
      val filesystem = FileSystem.get(new URI(fs + path), p.sc.hadoopConfiguration)
      val out = filesystem.create(new Path(path + "/serialized.dat"))
      ctw(s).write(out)
      out.close()
      filesystem.close()
    }
    def persistStorable() = s
  }


  implicit def dfToStorable(df: DataFrame): Storable[DataFrame] =
    new DataFrameStorable(df)
  implicit def rddToStorable[W: ClassTag, V <: RDD[W]](rdd: V): Storable[RDD[W]] =
    new RDDStorable[W](rdd)
  implicit def serializableToStorable[V <: Serializable: ClassTag](s: V): Storable[V] =
    new SerializableStorable[V](s)
  implicit def writableToStorable[V <: Writable: ClassTag](s: V): Storable[V] =
    new WritableConvertedStorable[V,V](s, v => v, w => w)
  implicit def doubleToStorable(s: Double): Storable[Double] =
    new WritableConvertedStorable[Double, DoubleWritable](s, new DoubleWritable(_), _.get())

}


trait Storable[V] {
  def readStorable(p: Peapod, fs: String, path: String): V
  def writeStorable(p: Peapod, fs: String, path: String)
  def persistStorable(): V
}


abstract class StorableTaskBase[V : ClassTag](implicit val p: Peapod)
  extends Task[V] with Logging  {
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
      writeSuccess()
      read()
    } else {
      logInfo("Loading" + dir + " Reading")
      read()
    }
    if(shouldPersist()) {
      logInfo("Loading" + dir + " Persisting")
      persist(rdd)
    } else {
      rdd
    }
  }
  protected def read(): V
  protected def write(v: V): Unit
  protected def persist(v: V): V
  private def writeSuccess(): Unit = {
    val filesystem = FileSystem.get(new URI(dir), p.sc.hadoopConfiguration)
    filesystem.createNewFile(new Path(dir + "/_SUCCESS"))
    filesystem.close()
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

abstract class StorableTask[V : ClassTag](implicit p: Peapod, c: V => Storable[V])
  extends StorableTaskBase[V] {

  protected def read(): V = {
    c(null.asInstanceOf[V])
      .readStorable(p,p.fs,p.path + "/" + name + "/" + recursiveVersionShort)
  }
  protected def write(v: V): Unit = {
    v.writeStorable(p,p.fs,p.path + "/" + name + "/" + recursiveVersionShort)
  }
  protected def persist(v: V): V = {
    v.persistStorable()
  }


}
