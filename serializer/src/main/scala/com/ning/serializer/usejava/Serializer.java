package com.ning.serializer.usejava;


import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/9/29
 * Time: 21:18
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public abstract class Serializer {
    //默认类加载器
    protected Optional<ClassLoader> defaultClassLoader = Optional.empty();
    public void setDefaultClassLoader( Optional<ClassLoader> defaultClassLoader ){
        this.defaultClassLoader = defaultClassLoader;
    }
    public abstract SerializerInstance newInstance();

}
abstract class SerializerInstance {
    public  abstract <T> ByteBuffer serialize(T t);

    public abstract <T> T deserialize (ByteBuffer bytes );

    public abstract <T> T deserialize(ByteBuffer bytes, ClassLoader loader);

    public abstract SerializationStream serializeStream(OutputStream s );

    public abstract DeserializationStream deserializeStream(InputStream s) ;

    public abstract DeserializationStream deserializeStream(InputStream s,ClassLoader classLoader) ;
}
abstract class SerializationStream {
    /** The most general-purpose method to write an object. */
    public abstract <T> SerializationStream writeObject(T t);
    /** Writes the object representing the key of a key-value pair. */
    public <T> SerializationStream  writeKey( T key) {
        return writeObject(key);
    }
    /** Writes the object representing the value of a key-value pair. */
    public <T> SerializationStream writeValue(T value){
        return writeObject(value);
    }
    public abstract void  flush();
    public abstract void  close();

    public <T> SerializationStream writeAll(Iterator<T> iter) {
        while (iter.hasNext()) {
            writeObject(iter.next());
        }
        return this;
    }
}
abstract class DeserializationStream {
    /**
     * The most general-purpose method to read an object.
     */
    public abstract <T> T readObject();

    /**
     * Reads the object representing the key of a key-value pair.
     */
    public <T> T readKey() {
        return readObject();
    }

    /**
     * Reads the object representing the value of a key-value pair.
     */
    public <T> T readValue() {
        return readObject();
    }

    public abstract void close();

    /**
     * Read the elements of this stream through an iterator. This can only be called once, as
     * reading each element will consume data from the input source.
     */
    public  <T> Iterator<T> asIterator(){
        return new NextIterator<T>() {
            @Override
            public T getNext() {
                try{
                    return readObject();
                }catch(Exception eof){
                    if(eof instanceof  EOFException){
                        finished = true ;
                    }
                    return null;
                }
            }

            @Override
            protected void close() {
                DeserializationStream.this.close();
            }
        };
    }

}