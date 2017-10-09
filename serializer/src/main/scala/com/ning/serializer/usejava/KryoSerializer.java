package com.ning.serializer.usejava;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.twitter.chill.EmptyScalaKryoInstantiator;
import com.twitter.chill.KryoBase;

import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/10/5
 * Time: 22:43
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public class KryoSerializer extends Serializer{
    private int bufferSize = 64 * 1024 * 1024 ;
    private int maxBufferSize = 64 * 1024 * 1028 * 1024 ;
    private boolean referenceTracking = true;
    private boolean registrationRequired = false;
    public Output newKryoOutput(){
        return new Output(bufferSize,Math.max(bufferSize,maxBufferSize));
    }
    public Kryo newKryo(){
        Kryo kryo = new Kryo();
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader classLoader = defaultClassLoader.orElse(Thread.currentThread().getContextClassLoader());
        kryo.setReferences(referenceTracking);
        return kryo;
    }
    @Override
    public SerializerInstance newInstance() {
        return new KryoSerializerInstance(this);
    }
}
class KryoSerializationStream extends SerializationStream{
    private Output out ;
    private Kryo kryo ;

    public KryoSerializationStream(KryoSerializerInstance serInstance,OutputStream outStream){
        out = new Output(outStream);
        kryo = serInstance;
    }
}
class KryoSerializerInstance extends SerializerInstance{
    private Kryo cachedKryo;
    private KryoSerializer ks ;
    private Output output ;
    private Input input ;
    public KryoSerializerInstance(KryoSerializer ks){
        this.ks = ks;
        this.output = ks.newKryoOutput();
        this.input = new Input();
        cachedKryo = borrowKryo();
    }
    public Kryo borrowKryo(){
        if(cachedKryo != null){
            Kryo kryo = cachedKryo;
            kryo.reset();
            cachedKryo = null;
            return kryo;
        }else{
            return ks.newKryo();
        }
    }
    public void releaseKryo(Kryo kryo){
        if(cachedKryo == null){
            cachedKryo = kryo;
        }
    }
    public ByteBuffer serialize(Object obj){
        output.clear();
        Kryo kryo = borrowKryo();
        kryo.writeClassAndObject(output,obj);
        releaseKryo(kryo);
        return ByteBuffer.wrap(output.toBytes());
    }

    @Override
    public <T> T deserialize(ByteBuffer bytes) {
        Kryo kryo = borrowKryo();
        ClassLoader oldClassLoader = kryo.getClassLoader();
        input.setBuffer(bytes.array());
        Object obj = kryo.readClassAndObject(input);
        releaseKryo(kryo);
        kryo.setClassLoader(oldClassLoader);
        releaseKryo(kryo);
        return (T)obj;
    }


}
