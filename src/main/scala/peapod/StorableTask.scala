package peapod

import java.io.{ByteArrayOutputStream, IOException, ObjectInputStream, ObjectOutputStream}
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.reflect.{ClassTag, _}
import scala.reflect.runtime.universe._

/**
  * Helper object that provides methods and classes for allowing various objects to be automatically saved by the
  * StorableTask classes
  */
object StorableTask {
  /**
    * Helper method for serializing objects into a byte array
    * @param o Object to be serialized
    * @tparam T Type to be serialized
    * @return Byte array of the serialized object
    */
  private def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  /**
    * Helper function for saving and RDD of arbitrary objects to disk with compression, by default Spark only saves
    * RDDs to disk without compression.
    * @param rdd RDD to be saved in a compressed state
    * @param path The path where the RDD is saved
    */

  private def saveAsCompressedObjectFile(rdd: RDD[_], path: String): Unit = {
    rdd.mapPartitions(iter => iter.grouped(10).map(_.toArray))
      .map(x => (NullWritable.get(), new BytesWritable(StorableTask.serialize(x))))
      .saveAsSequenceFile(path, Some(classOf[BZip2Codec]))
  }

  /**
    * Reads and writes a DataFrame from disc
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

  /**
    * Reads and writes a DataSet from disc
    * @param ds DataSet that is being extended
    * @tparam W The type of the DataSet that is being stored, must be of type Product
    */
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

  /**
    * Reads and writes a RDD from disc
    * @param rdd RDD that is being extended
    * @tparam W The type of the RDD that is being stored
    */
  class RDDStorable[W: ClassTag](rdd: RDD[W]) extends Storable[RDD[W]] {
    def readStorable(p: Peapod, dir: String): RDD[W] = {
      p.sc.objectFile[W](dir)
    }
    def writeStorable(p: Peapod, dir: String) = {
      StorableTask.saveAsCompressedObjectFile(rdd, dir)
    }
  }

  /**
    * Reads and writes a Serializable objects from disc
    * @param s Object that is being extended
    * @tparam V The type of the Object that is being stored, must extend Serializable
    */
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

  /**
    * Reads and writes a Writable objects from disc
    * @param s Object that is being extended
    * @param ctw Function that converts Objects of type V to a matching Writable of type W
    * @param wtc Function that converts Writables of type W to their matching objects of type V
    * @tparam V The type of the Object that is being stored
    * @tparam W The type of the Object's Writable interface that is being stored
    */
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


  /**
    * Wraps a DataFrame into a Storable
    */
  implicit def dfToStorable(df: DataFrame): Storable[DataFrame] =
    new DataFrameStorable(df)
  /**
    * Wraps a Dataset into a Storable
    */
  implicit def dsToStorable[W <: Product : TypeTag, V <: Dataset[W]](ds: V): Storable[Dataset[W]] =
    new DataSetStorable[W](ds)
  /**
    * Wraps a RDD into a Storable
    */
  implicit def rddToStorable[W: ClassTag, V <: RDD[W]](rdd: V): Storable[RDD[W]] =
    new RDDStorable[W](rdd)
  /**
    * Wraps a Serializable into a Storable
    */
  implicit def serializableToStorable[V <: Serializable: ClassTag](s: V): Storable[V] =
    new SerializableStorable[V](s)
  /**
    * Wraps a Writable into a Storable
    */
  implicit def writableToStorable[V <: Writable: ClassTag](s: V): Storable[V] =
    new WritableConvertedStorable[V,V](s, v => v, w => w)
  /**
    * Wraps a Double into a Storable
    */
  implicit def doubleToStorable(s: Double): Storable[Double] =
    new WritableConvertedStorable[Double, DoubleWritable](s, new DoubleWritable(_), _.get())

}

/**
  * Helper trait for allowing objects of type V to be serializaed and de-serialized
  */
trait Storable[V] {
  def readStorable(p: Peapod, dir: String): V
  def writeStorable(p: Peapod, dir: String)
}

/**
  * A base Task class which store's it's output to disk automatically based on the Peapod's path variable. It does not
  * use implicits for automatic type serialization and so must have the write and read methods specified manually in
  * extending classes.
  */
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
    val exists = if (classTag[V] == classTag[DataFrame]) {
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
    fs.close()
    exists
  }
}

/**
  * A base Task class which store's it's output to disk automatically based on the Peapod's path variable. It uses
  * implicits from the StorableTask object for automatically allowing output objects to be serialized based on their
  * type.
  */
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
