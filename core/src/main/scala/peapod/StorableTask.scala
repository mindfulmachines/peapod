package peapod

import java.io.{IOException, ObjectInputStream, ObjectOutputStream, ByteArrayOutputStream}
import java.net.URI

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.hadoop.io._
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.storage.StorageLevel
import scala.reflect._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

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

  /**
    * Reads and write a DataFrame from disc
    * @param df DataFrame that is being extended
    */
  class DataFrameStorable(df: DataFrame) extends Storable[DataFrame] with Logging {
    def readStorable(p: Peapod, dir: String): DataFrame = {
      if(dir.startsWith("s3n")) {
        //There's a bug in the parquet reader for S3 so it doesn't properly get the hadoop configuration key and secret
        val uri = new URI(dir)
        val awsKey = p.sc.hadoopConfiguration.get("fs.s3n.awsAccessKeyId")
        val awsSecret = p.sc.hadoopConfiguration.get("fs.s3n.awsSecretAccessKey")
        p.sqlCtx.read.parquet(uri.getScheme + "://" +  awsKey + ":" + awsSecret + "@" + uri.getHost + uri.getPath)
      } else {
          p.sqlCtx.read.parquet(dir)
      }
    }
    def writeStorable(p: Peapod, dir: String) = {
      df.write.parquet(dir)

      //This is to deal with a bug where Parquet does not write the metadata files but only throws a Warning
      val metadata = new Path(dir + "/_metadata")
      val common = new Path(dir + "/_common_metadata")
      val fs = FileSystem.get(new URI(dir), p.sc.hadoopConfiguration)
      var retry = 0
      while(retry < 5 &&
        (!fs.exists(metadata) || !fs.isFile(metadata) || !fs.exists(common) || !fs.isFile(common))) {
        retry+=1
        Thread.sleep(10000)
        logWarning("Rerun starting for " + dir)
        fs.delete(new Path(dir), true)
        df.write.parquet(dir)
      }
      if (!fs.exists(metadata) || !fs.isFile(metadata) || !fs.exists(common) || !fs.isFile(common)) {
        new IOException("Could not write Parquet files")
      }
    }
  }

  class DataSetStorable[W <: Product : TypeTag](ds: Dataset[W]) extends Storable[Dataset[W]] {
    def readStorable(p: Peapod, dir: String): Dataset[W] = {
      import p.sqlCtx.implicits._
      if(dir.startsWith("s3n")) {
        //There's a bug in the parquet reader for S3 so it doesn't properly get the hadoop configuration key and secret
        val uri = new URI(dir)
        val awsKey = p.sc.hadoopConfiguration.get("fs.s3n.awsAccessKeyId")
        val awsSecret = p.sc.hadoopConfiguration.get("fs.s3n.awsSecretAccessKey")
        p.sqlCtx.read.parquet(uri.getScheme + "://" +  awsKey + ":" + awsSecret + "@" + uri.getHost + uri.getPath).as[W]
      } else {
        p.sqlCtx.read.parquet(dir).as[W]
      }
    }
    def writeStorable(p: Peapod, dir: String) = {
      ds.toDF().write.parquet(dir)
    }
  }

  class RDDStorable[W: ClassTag](rdd: RDD[W]) extends Storable[RDD[W]] {
    def readStorable(p: Peapod, dir: String): RDD[W] = {
      p.sc.objectFile[W](dir)
    }
    def writeStorable(p: Peapod, dir: String) = {
      StorableTask.saveAsCompressedObjectFile(rdd, dir)
    }
  }

  class SerializableStorable[V <: Serializable: ClassTag](s: V) extends Storable[V] {
    def readStorable(p: Peapod, dir: String): V = {
      val filesystem = FileSystem.get(new URI(dir), p.sc.hadoopConfiguration)
      val in = filesystem.open(new Path(dir+ "/serialized.dat"))
      val objReader = new ObjectInputStream(in)
      val obj = objReader.readObject().asInstanceOf[V]
      in.close()
      filesystem.close()
      obj
    }
    def writeStorable(p: Peapod, dir: String) = {
      val filesystem = FileSystem.get(new URI(dir), p.sc.hadoopConfiguration)
      val out = filesystem.create(new Path(dir + "/serialized.dat"))
      val objWriter = new ObjectOutputStream(out)
      objWriter.writeObject(s)
      objWriter.close()
      filesystem.close()
    }
  }

  class WritableConvertedStorable[V : ClassTag, W <: Writable: ClassTag]
      (s: V, ctw: V => W, wtc: W => V) extends Storable[V] {
    def readStorable(p: Peapod, dir: String): V = {
      val filesystem = FileSystem.get(new URI(dir), p.sc.hadoopConfiguration)
      val in = filesystem.open(new Path(dir + "/serialized.dat"))
      val obj = classTag[W].runtimeClass.newInstance().asInstanceOf[W]
      obj.readFields(in)
      in.close()
      filesystem.close()
      wtc(obj)
    }
    def writeStorable(p: Peapod, dir: String) = {
      val filesystem = FileSystem.get(new URI(dir), p.sc.hadoopConfiguration)
      val out = filesystem.create(new Path(dir + "/serialized.dat"))
      ctw(s).write(out)
      out.close()
      filesystem.close()
    }
  }


  implicit def dfToStorable(df: DataFrame): Storable[DataFrame] =
    new DataFrameStorable(df)
  implicit def dsToStorable[W <: Product : TypeTag, V <: Dataset[W]](ds: V): Storable[Dataset[W]] =
    new DataSetStorable[W](ds)
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
  def readStorable(p: Peapod, dir: String): V
  def writeStorable(p: Peapod, dir: String)
}


abstract class StorableTaskBase[V : ClassTag]
  extends Task[V] with Logging  {
  protected def generate: V
  val storable = true

  def build(): V = {
    write(generate)
    writeSuccess()
    read()
  }
  def load(): V = {
    read()
  }

  protected def read(): V
  protected def write(v: V): Unit


  private def writeSuccess(): Unit = {
    val filesystem = FileSystem.get(new URI(dir), p.sc.hadoopConfiguration)
    filesystem.createNewFile(new Path(dir + "/_SUCCESS"))
    filesystem.close()
  }
  def delete() {
    val fs = FileSystem.get(new URI(dir), p.sc.hadoopConfiguration)
    fs.delete(new Path(dir), true)
  }
  def exists(): Boolean = {
    val fs = FileSystem.get(new URI(dir), p.sc.hadoopConfiguration)
    val path = new Path(dir + "/_SUCCESS")
    if (classTag[V] == classTag[DataFrame]) {
      //This is to deal with a bug where Parquet does not write the metadata files
      //This cleans up any directories which were corrupted by the bug
      val path2 = new Path(dir + "/_metadata")
      val path3 = new Path(dir + "/_common_metadata")
      fs.exists(path) && fs.isFile(path) &&
        fs.exists(path2) && fs.isFile(path2)  &&
        fs.exists(path3) && fs.isFile(path3)
    } else {
      fs.exists(path) && fs.isFile(path)
    }

  }
}

abstract class StorableTask[V : ClassTag](implicit c: V => Storable[V])
  extends StorableTaskBase[V] {

  protected def read(): V = {
    c(null.asInstanceOf[V])
      .readStorable(p,dir)
  }
  protected def write(v: V): Unit = {
    v.writeStorable(p,dir)
  }

}
