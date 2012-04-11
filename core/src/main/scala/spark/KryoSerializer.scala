package spark

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.Channels

import scala.collection.immutable
import scala.collection.mutable

import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Serializer => KSerializer}

import java.lang.reflect.Constructor
import sun.reflect.ReflectionFactory


class KryoSerializationStream(kryo: Kryo, bufferSize: Int, outStream: OutputStream)
extends SerializationStream {
  // Each stream needs its own output buffer.
  val output = new Output(outStream, bufferSize)

  def writeObject[T](t: T) {
    output.setOutputStream(outStream)
    kryo.writeClassAndObject(output, t)
    output.flush()
  }

  def flush() { outStream.flush() }
  def close() { outStream.close() }
}


class KryoDeserializationStream(kryo: Kryo, bufferSize: Int, inStream: InputStream)
extends DeserializationStream {
  // Each stream needs its own input buffer.
  val input = new Input(inStream, bufferSize)

  def readObject[T](): T = {
    kryo.readClassAndObject(input).asInstanceOf[T]
  }

  def close() { inStream.close() }
}


class KryoSerializerInstance(val kryo: Kryo, val bufferSize: Int) extends SerializerInstance {
  
  val output = new Output(bufferSize, bufferSize)

  def serialize[T](t: T): Array[Byte] = {
    output.clear()
    kryo.writeClassAndObject(output, t)
    output.toBytes()
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    val input = new Input(bytes)
    val obj = kryo.readClassAndObject(input).asInstanceOf[T]
    input.rewind()
    obj
  }

  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    kryo.setClassLoader(loader)
    deserialize[T](bytes)
  }

  def outputStream(s: OutputStream): SerializationStream = {
    new KryoSerializationStream(kryo, bufferSize, s)
  }

  def inputStream(s: InputStream): DeserializationStream = {
    new KryoDeserializationStream(kryo, bufferSize, s)
  }
}


/**
 * Used by clients to register their own classes.
 */
trait KryoRegistrator {
  def registerClasses(kryo: Kryo): Unit
}


/**
 * Provides support for deserializing classes without a default constructor.
 * Code taken from Martin Grotzke's kyro-serializers project.
 */
object KryoReflectionFactorySupport {
  private val REFLECTION_FACTORY: ReflectionFactory = ReflectionFactory.getReflectionFactory()
  private val INITARGS = Seq[java.lang.Object]()
  private val _constructors = new java.util.concurrent.ConcurrentHashMap[Class[_], Constructor[_]]()
  
  private def newInstanceFrom(constructor: Constructor[_]): Any = {
    try {
      return constructor.newInstance()
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }
  
  private def newConstructorForSerialization[T](cls: Class[T]): Constructor[_] = {
    try {
      val constructor: Constructor[_] = REFLECTION_FACTORY.newConstructorForSerialization(
          cls, classOf[Object].getDeclaredConstructor())
      constructor.setAccessible(true)
      return constructor
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }
  
  private def getNoArgsConstructor( cls: Class[_] ): Constructor[_] = {
    val constructors = cls.getConstructors()
    constructors.foreach { constructor => {
      if ( constructor.getParameterTypes().length == 0 ) {
        constructor.setAccessible(true)
        return constructor
      }
    }}
    return null
  }
}


/**
 * Provides support for deserializing classes without a default constructor.
 * Code taken from Martin Grotzke's kyro-serializers project.
 */
class KryoReflectionFactorySupport extends Kryo {
  override def newInstance[T](cls: Class[T]): T = {
    var constructor = KryoReflectionFactorySupport._constructors.get(cls)
    if (constructor == null) {
      constructor = KryoReflectionFactorySupport.getNoArgsConstructor(cls)
      if (constructor == null) {
        constructor = KryoReflectionFactorySupport.newConstructorForSerialization(cls)
      }
      KryoReflectionFactorySupport._constructors.put(cls, constructor)
    }
    return KryoReflectionFactorySupport.newInstanceFrom(constructor).asInstanceOf[T]
  }
}


class KryoSerializer extends Serializer with Logging {
  
  val kryo = new ThreadLocal[Kryo] {
    override def initialValue = createKryo()
  }

  val bufferSize = 
    System.getProperty("spark.kryoserializer.buffer.mb", "20").toInt * 1024 * 1024

  def createKryo(): Kryo = {
    // This is used so we can serialize/deserialize objects without a zero-arg
    // constructor.
    val kryo = new KryoReflectionFactorySupport()

    // Register some commonly used Scala singleton objects. Because these
    // are singletons, we must return the exact same local object when we
    // deserialize rather than returning a clone as FieldSerializer would.
    class SingletonSerializer(obj: AnyRef) extends KSerializer[AnyRef] {
      override def write(kryo: Kryo, output: Output, obj: AnyRef) {}
      override def read (kryo: Kryo, input: Input, cls: Class[AnyRef]): AnyRef = obj.asInstanceOf[AnyRef]
    }
    kryo.register(None.getClass, new SingletonSerializer(None))
    kryo.register(Nil.getClass, new SingletonSerializer(Nil))

    // Register maps with a special serializer since they have complex internal structure
    class ScalaMapSerializer(buildMap: Array[(Any, Any)] => scala.collection.Map[Any, Any])
    extends KSerializer[Array[(Any, Any)] => scala.collection.Map[Any, Any]] {
      override def write(kryo: Kryo, output: Output, obj: Array[(Any, Any)] => scala.collection.Map[Any, Any]) {
        val map = obj.asInstanceOf[scala.collection.Map[Any, Any]]
        kryo.writeObject(output, map.size.asInstanceOf[java.lang.Integer])
        for ((k, v) <- map) {
          kryo.writeClassAndObject(output, k)
          kryo.writeClassAndObject(output, v)
        }
      }
      override def read (
        kryo: Kryo,
        input: Input,
        cls: Class[Array[(Any, Any)] => scala.collection.Map[Any, Any]])
      : Array[(Any, Any)] => scala.collection.Map[Any, Any] = {
        val size = kryo.readObject(input, classOf[java.lang.Integer]).intValue
        val elems = new Array[(Any, Any)](size)
        for (i <- 0 until size)
          elems(i) = (kryo.readClassAndObject(input), kryo.readClassAndObject(input))
        buildMap(elems).asInstanceOf[Array[(Any, Any)] => scala.collection.Map[Any, Any]]
      }
    }
    kryo.register(mutable.HashMap().getClass, new ScalaMapSerializer(mutable.HashMap() ++ _))
    // TODO: add support for immutable maps too; this is more annoying because there are many
    // subclasses of immutable.Map for small maps (with <= 4 entries)
    val map1  = Map[Any, Any](1 -> 1)
    val map2  = Map[Any, Any](1 -> 1, 2 -> 2)
    val map3  = Map[Any, Any](1 -> 1, 2 -> 2, 3 -> 3)
    val map4  = Map[Any, Any](1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4)
    val map5  = Map[Any, Any](1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4, 5 -> 5)
    kryo.register(map1.getClass, new ScalaMapSerializer(mutable.HashMap() ++ _ toMap))
    kryo.register(map2.getClass, new ScalaMapSerializer(mutable.HashMap() ++ _ toMap))
    kryo.register(map3.getClass, new ScalaMapSerializer(mutable.HashMap() ++ _ toMap))
    kryo.register(map4.getClass, new ScalaMapSerializer(mutable.HashMap() ++ _ toMap))
    kryo.register(map5.getClass, new ScalaMapSerializer(mutable.HashMap() ++ _ toMap))

    // Allow the user to register their own classes by setting spark.kryo.registrator
    val regCls = System.getProperty("spark.kryo.registrator")
    if (regCls != null) {
      logInfo("Running user registrator: " + regCls)
      val reg = Class.forName(regCls).newInstance().asInstanceOf[KryoRegistrator]
      reg.registerClasses(kryo)
    }
    kryo
  }

  def newInstance(): SerializerInstance = new KryoSerializerInstance(kryo.get, bufferSize)
}

