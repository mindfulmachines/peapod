package peapod

import java.io.{ByteArrayOutputStream, IOException}
import java.util.zip.GZIPOutputStream

import org.apache.commons.codec.binary.Base64
import org.apache.commons.codec.net.URLCodec

/**
  * Helper class
  */
object Util {
  @throws(classOf[IOException])
  def compress(data: String): Array[Byte] = {
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream(data.length)
    val gzip: GZIPOutputStream = new GZIPOutputStream(bos)
    gzip.write(data.getBytes)
    gzip.close()
    bos.close()
    val compressed: Array[Byte] = bos.toByteArray
    compressed
  }

  @throws(classOf[IOException])
  def compress(data: Array[Byte]): Array[Byte] = {
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream(data.length)
    val gzip: GZIPOutputStream = new GZIPOutputStream(bos)
    gzip.write(data)
    gzip.close()
    bos.close()
    val compressed: Array[Byte] = bos.toByteArray
    compressed
  }

  def gravizoDotLink(dot: String): String = {
    "http://g.gravizo.com/g?" +
      new URLCodec().encode(dot).replace("+","%20")
  }

  def mindfulmachinesDotLink(dot: String): String = {
    "http://graphvizserver-env.elasticbeanstalk.com/?" +
      new URLCodec().encode(
        Base64.encodeBase64URLSafeString(
          Util.compress(
            dot
          )
        ))
  }
}
