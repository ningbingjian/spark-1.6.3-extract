/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ning.serializer.usescala

import java.io._
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}
import com.esotericsoftware.kryo.{Kryo, KryoException, Serializer => KryoClassSerializer}
import com.twitter.chill.{AllScalaRegistrar, EmptyScalaKryoInstantiator}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.roaringbitmap.RoaringBitmap


/**
 * A Spark serializer that uses the [[https://code.google.com/p/kryo/ Kryo serialization library]].
 *
 * Note that this serializer is not guaranteed to be wire-compatible across different versions of
 * Spark. It is intended to be used to serialize/de-serialize data within a single
 * Spark application.
 */
class KryoSerializer()
  extends Serializer
  with Serializable {

  private val bufferSize = 64  * 1024

  private val maxBufferSize = 64 * 1024 * 1024

  private val referenceTracking = true
  private val registrationRequired = false
  private val classesToRegister = ""
    .split(',')
    .filter(!_.isEmpty)


  def newKryoOutput(): KryoOutput = new KryoOutput(bufferSize, math.max(bufferSize, maxBufferSize))

  def newKryo(): Kryo = {
    val instantiator = new EmptyScalaKryoInstantiator
    val kryo = instantiator.newKryo()
    kryo.setRegistrationRequired(registrationRequired)

    val oldClassLoader = Thread.currentThread.getContextClassLoader
    val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)

    // Allow disabling Kryo reference tracking if user knows their object graphs don't have loops.
    // Do this before we invoke the user registrator so the user registrator can override this.
    kryo.setReferences(referenceTracking)

    // For results returned by asJavaIterable. See JavaIterableWrapperSerializer.
    kryo.register(JavaIterableWrapperSerializer.wrapperClass, new JavaIterableWrapperSerializer)
    kryo.setClassLoader(classLoader)
    kryo
  }

  override def newInstance(): SerializerInstance = {
    new KryoSerializerInstance(this)
  }
}

private
class KryoSerializationStream(
    serInstance: KryoSerializerInstance,
    outStream: OutputStream) extends SerializationStream {

  private var output: KryoOutput = new KryoOutput(outStream)
  private var kryo: Kryo = serInstance.borrowKryo()

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    kryo.writeClassAndObject(output, t)
    this
  }

  override def flush() {
    if (output == null) {
      throw new IOException("Stream is closed")
    }
    output.flush()
  }

  override def close() {
    if (output != null) {
      try {
        output.close()
      } finally {
        serInstance.releaseKryo(kryo)
        kryo = null
        output = null
      }
    }
  }
}

private
class KryoDeserializationStream(
    serInstance: KryoSerializerInstance,
    inStream: InputStream) extends DeserializationStream {

  private var input: KryoInput = new KryoInput(inStream)
  private var kryo: Kryo = serInstance.borrowKryo()

  override def readObject[T: ClassTag](): T = {
    try {
      kryo.readClassAndObject(input).asInstanceOf[T]
    } catch {
      // DeserializationStream uses the EOF exception to indicate stopping condition.
      case e: KryoException if e.getMessage.toLowerCase.contains("buffer underflow") =>
        throw new EOFException
    }
  }

  override def close() {
    if (input != null) {
      try {
        // Kryo's Input automatically closes the input stream it is using.
        input.close()
      } finally {
        serInstance.releaseKryo(kryo)
        kryo = null
        input = null
      }
    }
  }
}

private class KryoSerializerInstance(ks: KryoSerializer) extends SerializerInstance {

  /**
   * A re-used [[Kryo]] instance. Methods will borrow this instance by calling `borrowKryo()`, do
   * their work, then release the instance by calling `releaseKryo()`. Logically, this is a caching
   * pool of size one. SerializerInstances are not thread-safe, hence accesses to this field are
   * not synchronized.
   */
  private var cachedKryo: Kryo = borrowKryo()

  /**
   * Borrows a [[Kryo]] instance. If possible, this tries to re-use a cached Kryo instance;
   * otherwise, it allocates a new instance.
   */
  def borrowKryo(): Kryo = {
    if (cachedKryo != null) {
      val kryo = cachedKryo
      // As a defensive measure, call reset() to clear any Kryo state that might have been modified
      // by the last operation to borrow this instance (see SPARK-7766 for discussion of this issue)
      kryo.reset()
      cachedKryo = null
      kryo
    } else {
      ks.newKryo()
    }
  }

  /**
   * Release a borrowed [[Kryo]] instance. If this serializer instance already has a cached Kryo
   * instance, then the given Kryo instance is discarded; otherwise, the Kryo is stored for later
   * re-use.
   */
  def releaseKryo(kryo: Kryo): Unit = {
    if (cachedKryo == null) {
      cachedKryo = kryo
    }
  }

  // Make these lazy vals to avoid creating a buffer unless we use them.
  private lazy val output = ks.newKryoOutput()
  private lazy val input = new KryoInput()

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    output.clear()
    val kryo = borrowKryo()
    try {
      kryo.writeClassAndObject(output, t)
    } catch {
      case e: KryoException if e.getMessage.startsWith("Buffer overflow") =>
        throw new RuntimeException(s"Kryo serialization failed: ${e.getMessage}. To avoid this, " +
          "increase spark.kryoserializer.buffer.max value.")
    } finally {
      releaseKryo(kryo)
    }
    ByteBuffer.wrap(output.toBytes)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val kryo = borrowKryo()
    try {
      input.setBuffer(bytes.array)
      kryo.readClassAndObject(input).asInstanceOf[T]
    } finally {
      releaseKryo(kryo)
    }
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val kryo = borrowKryo()
    val oldClassLoader = kryo.getClassLoader
    try {
      kryo.setClassLoader(loader)
      input.setBuffer(bytes.array)
      kryo.readClassAndObject(input).asInstanceOf[T]
    } finally {
      kryo.setClassLoader(oldClassLoader)
      releaseKryo(kryo)
    }
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new KryoSerializationStream(this, s)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new KryoDeserializationStream(this, s)
  }

  /**
   * Returns true if auto-reset is on. The only reason this would be false is if the user-supplied
   * registrator explicitly turns auto-reset off.
   */
  def getAutoReset(): Boolean = {
    val field = classOf[Kryo].getDeclaredField("autoReset")
    field.setAccessible(true)
    val kryo = borrowKryo()
    try {
      field.get(kryo).asInstanceOf[Boolean]
    } finally {
      releaseKryo(kryo)
    }
  }
}

/**
 * Interface implemented by clients to register their classes with Kryo when using Kryo
 * serialization.
 */
trait KryoRegistrator {
  def registerClasses(kryo: Kryo)
}

private[serializer] object KryoSerializer {

  private val toRegisterSerializer = Map[Class[_], KryoClassSerializer[_]](
    classOf[RoaringBitmap] -> new KryoClassSerializer[RoaringBitmap]() {
      override def write(kryo: Kryo, output: KryoOutput, bitmap: RoaringBitmap): Unit = {
        bitmap.serialize(new KryoOutputObjectOutputBridge(kryo, output))
      }
      override def read(kryo: Kryo, input: KryoInput, cls: Class[RoaringBitmap]): RoaringBitmap = {
        val ret = new RoaringBitmap
        ret.deserialize(new KryoInputObjectInputBridge(kryo, input))
        ret
      }
    }
  )
}

/**
 * This is a bridge class to wrap KryoInput as an InputStream and ObjectInput. It forwards all
 * methods of InputStream and ObjectInput to KryoInput. It's usually helpful when an API expects
 * an InputStream or ObjectInput but you want to use Kryo.
 */
private class KryoInputObjectInputBridge(
    kryo: Kryo, input: KryoInput) extends FilterInputStream(input) with ObjectInput {
  override def readLong(): Long = input.readLong()
  override def readChar(): Char = input.readChar()
  override def readFloat(): Float = input.readFloat()
  override def readByte(): Byte = input.readByte()
  override def readShort(): Short = input.readShort()
  override def readUTF(): String = input.readString() // readString in kryo does utf8
  override def readInt(): Int = input.readInt()
  override def readUnsignedShort(): Int = input.readShortUnsigned()
  override def skipBytes(n: Int): Int = {
    input.skip(n)
    n
  }
  override def readFully(b: Array[Byte]): Unit = input.read(b)
  override def readFully(b: Array[Byte], off: Int, len: Int): Unit = input.read(b, off, len)
  override def readLine(): String = throw new UnsupportedOperationException("readLine")
  override def readBoolean(): Boolean = input.readBoolean()
  override def readUnsignedByte(): Int = input.readByteUnsigned()
  override def readDouble(): Double = input.readDouble()
  override def readObject(): AnyRef = kryo.readClassAndObject(input)
}

/**
 * This is a bridge class to wrap KryoOutput as an OutputStream and ObjectOutput. It forwards all
 * methods of OutputStream and ObjectOutput to KryoOutput. It's usually helpful when an API expects
 * an OutputStream or ObjectOutput but you want to use Kryo.
 */
private class KryoOutputObjectOutputBridge(
    kryo: Kryo, output: KryoOutput) extends FilterOutputStream(output) with ObjectOutput  {
  override def writeFloat(v: Float): Unit = output.writeFloat(v)
  // There is no "readChars" counterpart, except maybe "readLine", which is not supported
  override def writeChars(s: String): Unit = throw new UnsupportedOperationException("writeChars")
  override def writeDouble(v: Double): Unit = output.writeDouble(v)
  override def writeUTF(s: String): Unit = output.writeString(s) // writeString in kryo does UTF8
  override def writeShort(v: Int): Unit = output.writeShort(v)
  override def writeInt(v: Int): Unit = output.writeInt(v)
  override def writeBoolean(v: Boolean): Unit = output.writeBoolean(v)
  override def write(b: Int): Unit = output.write(b)
  override def write(b: Array[Byte]): Unit = output.write(b)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = output.write(b, off, len)
  override def writeBytes(s: String): Unit = output.writeString(s)
  override def writeChar(v: Int): Unit = output.writeChar(v.toChar)
  override def writeLong(v: Long): Unit = output.writeLong(v)
  override def writeByte(v: Int): Unit = output.writeByte(v)
  override def writeObject(obj: AnyRef): Unit = kryo.writeClassAndObject(output, obj)
}

/**
 * A Kryo serializer for serializing results returned by asJavaIterable.
 *
 * The underlying object is scala.collection.convert.Wrappers$IterableWrapper.
 * Kryo deserializes this into an AbstractCollection, which unfortunately doesn't work.
 */
private class JavaIterableWrapperSerializer
  extends com.esotericsoftware.kryo.Serializer[java.lang.Iterable[_]] {

  import JavaIterableWrapperSerializer._

  override def write(kryo: Kryo, out: KryoOutput, obj: java.lang.Iterable[_]): Unit = {
    // If the object is the wrapper, simply serialize the underlying Scala Iterable object.
    // Otherwise, serialize the object itself.
    if (obj.getClass == wrapperClass && underlyingMethodOpt.isDefined) {
      kryo.writeClassAndObject(out, underlyingMethodOpt.get.invoke(obj))
    } else {
      kryo.writeClassAndObject(out, obj)
    }
  }

  override def read(kryo: Kryo, in: KryoInput, clz: Class[java.lang.Iterable[_]])
    : java.lang.Iterable[_] = {
    kryo.readClassAndObject(in) match {
      case scalaIterable: Iterable[_] => scalaIterable.asJava
      case javaIterable: java.lang.Iterable[_] => javaIterable
    }
  }
}

private object JavaIterableWrapperSerializer {
  // The class returned by JavaConverters.asJava
  // (scala.collection.convert.Wrappers$IterableWrapper).
  val wrapperClass =
    scala.collection.convert.WrapAsJava.asJavaIterable(Seq(1)).getClass

  // Get the underlying method so we can use it to get the Scala collection for serialization.
  private val underlyingMethodOpt = {
    try Some(wrapperClass.getDeclaredMethod("underlying")) catch {
      case e: Exception =>
        None
    }
  }
}
