package com.ning.serializer.usejava;

import com.ning.serializer.usescala.ByteBufferInputStream;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/9/29
 * Time: 22:19
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public class JavaSerializer extends Serializer{
    private int countReset = 100 ;
    private boolean extractDebugInfo = true;

    @Override
    public SerializerInstance newInstance() {
        ClassLoader classLoader =  defaultClassLoader.orElse(Thread.currentThread().getContextClassLoader());
        return new JavaSerializerInstance(countReset,extractDebugInfo,classLoader);
    }

}

class JavaSerializationStream extends SerializationStream{
    private ObjectOutputStream objOut ;
    private int counter = 0 ;
    private int counterReset;
    private boolean extractDebugInfo ;
    public JavaSerializationStream(OutputStream out,int counterReset, boolean extraDebugInfo){
        try{
            this.objOut = new ObjectOutputStream(out);
        }catch (IOException ex){
            throw new RuntimeException(ex);
        }
        this.counterReset = counterReset;
        this.extractDebugInfo = extraDebugInfo;
    }

    @Override
    public <T> SerializationStream writeObject(T t) {
        try {
            objOut.writeObject(t);
        } catch (IOException e) {
            e.printStackTrace();
        }
        counter += 1 ;
        if (counterReset > 0 && counter >= counterReset) {
            try {
                objOut.reset();
            } catch (IOException e) {
                e.printStackTrace();
            }
            counter = 0 ;
        }
        return this;
    }

    @Override
    public void close() {
        try{
            objOut.close();
        }catch (IOException ex){

        }

    }

    @Override
    public void flush() {
        try{
            objOut.flush();
        }catch (IOException ex){

        }

    }
}
class JavaDeserializationStream extends DeserializationStream{
    private ObjectInputStream objIn;
    private ClassLoader classLoader ;
    public JavaDeserializationStream(InputStream in , final ClassLoader classLoader){
        this.classLoader = classLoader;
        try {
            this.objIn = new ObjectInputStream(in){
                @Override
                protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
                    return Class.forName(desc.getName(),true,classLoader);
                }
            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T readObject() {
        try {
            return (T)objIn.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            objIn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
class JavaSerializerInstance extends SerializerInstance{
    private int countReset;
    private boolean extractDebugInfo;
    private ClassLoader defaultClassLoader;
    public JavaSerializerInstance(int countReset,boolean extractDebugInfo,ClassLoader defaultClassLoader){
        this.countReset = countReset;
        this.extractDebugInfo = extractDebugInfo;
        this.defaultClassLoader = defaultClassLoader;
    }
    @Override
    public <T> ByteBuffer serialize(T t) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        SerializationStream out = serializeStream(bos);
        out.writeObject(t);
        out.close();
        return ByteBuffer.wrap(bos.toByteArray());
    }

    @Override
    public SerializationStream serializeStream(OutputStream out) {
        return new JavaSerializationStream(out,countReset,extractDebugInfo);
    }
    @Override
    public <T> T deserialize(ByteBuffer bytes) {
        ByteBufferInputStream bis = new ByteBufferInputStream(bytes,true);
        DeserializationStream in = deserializeStream(bis);
        return in.readObject();
    }

    @Override
    public <T> T deserialize(ByteBuffer bytes, ClassLoader loader) {
        ByteBufferInputStream bis = new ByteBufferInputStream(bytes,true);
        DeserializationStream in = deserializeStream(bis,loader);
        return in.readObject();
    }



    @Override
    public DeserializationStream deserializeStream(InputStream in) {
        return new JavaDeserializationStream(in,defaultClassLoader);
    }

    @Override
    public DeserializationStream deserializeStream(InputStream in, ClassLoader classLoader) {
        return new JavaDeserializationStream(in,classLoader);
    }
}