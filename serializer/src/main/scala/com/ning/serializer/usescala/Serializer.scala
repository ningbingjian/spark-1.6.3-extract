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

import scala.reflect.ClassTag

/**
  * 序列化工具类 主要负责返回序列化实例 类似Java工厂类
  */
abstract class Serializer {
  //默认类加载器
  @volatile protected var defaultClassLoader: Option[ClassLoader] = None

  /**
   * Sets a class loader for the serializer to use in deserialization.
   *
   * @return this Serializer object
   */
  def setDefaultClassLoader(classLoader: ClassLoader): Serializer = {
    defaultClassLoader = Some(classLoader)
    this
  }

  /** Creates a new [[SerializerInstance]]. */
  def newInstance(): SerializerInstance

}


object Serializer {
  def getSerializer(serializer: Serializer): Serializer = {
      serializer
  }

  def getSerializer(serializer: Option[Serializer]): Serializer = {
    serializer.get
  }
}


/**
 * :: DeveloperApi ::
 * An instance of a serializer, for use by one thread at a time.
 *
 * It is legal to create multiple serialization / deserialization streams from the same
 * SerializerInstance as long as those streams are all used within the same thread.
  *
  * 序列化实例
  * 定义实例的抽象方法 包括序列化对象，反序列化对象 将输出流包装为序列化流 将输入流包装为序列化流
 */
abstract class SerializerInstance {
  def serialize[T: ClassTag](t: T): ByteBuffer

  def deserialize[T: ClassTag](bytes: ByteBuffer): T

  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T

  def serializeStream(s: OutputStream): SerializationStream

  def deserializeStream(s: InputStream): DeserializationStream
}

/**
 * :: DeveloperApi ::
 * A stream for writing serialized objects.
  *
  * 序列化流  负责将对象写出到序列化流
  * */
abstract class SerializationStream {
  /** The most general-purpose method to write an object. */
  def writeObject[T: ClassTag](t: T): SerializationStream
  /** Writes the object representing the key of a key-value pair. */
  def writeKey[T: ClassTag](key: T): SerializationStream = writeObject(key)
  /** Writes the object representing the value of a key-value pair. */
  def writeValue[T: ClassTag](value: T): SerializationStream = writeObject(value)
  def flush(): Unit
  def close(): Unit

  def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
    while (iter.hasNext) {
      writeObject(iter.next())
    }
    this
  }
}


/**
 * :: DeveloperApi ::
 * A stream for reading serialized objects.
  * 反序列化抽象 负责将对象反序列化为JAVA对象
 */
abstract class DeserializationStream {
  /** The most general-purpose method to read an object. */
  def readObject[T: ClassTag](): T
  /** Reads the object representing the key of a key-value pair. */
  def readKey[T: ClassTag](): T = readObject[T]()
  /** Reads the object representing the value of a key-value pair. */
  def readValue[T: ClassTag](): T = readObject[T]()
  def close(): Unit

  /**
   * Read the elements of this stream through an iterator. This can only be called once, as
   * reading each element will consume data from the input source.
    *
   */
  def asIterator: Iterator[Any] = new NextIterator[Any] {
    override protected def getNext() = {
      try {
        readObject[Any]()
      } catch {
        case eof: EOFException =>
          finished = true
          null
      }
    }

    override protected def close() {
      DeserializationStream.this.close()
    }
  }

  /**
   * Read the elements of this stream through an iterator over key-value pairs. This can only be
   * called once, as reading each element will consume data from the input source.
   */
  def asKeyValueIterator: Iterator[(Any, Any)] = new NextIterator[(Any, Any)] {
    override protected def getNext() = {
      try {
        (readKey[Any](), readValue[Any]())
      } catch {
        case eof: EOFException => {
          finished = true
          null
        }
      }
    }

    override protected def close() {
      DeserializationStream.this.close()
    }
  }
}
