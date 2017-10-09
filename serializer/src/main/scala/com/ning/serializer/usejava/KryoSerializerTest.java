package com.ning.serializer.usejava;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/9/28
 * Time: 21:12
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public class KryoSerializerTest {
    public static void main(String[] args) throws Exception {
        testByte();
        testShort();
        testInt();
        testLong();
        testFloat();
        testDouble();
        testString();
        testDate();
        testPojo();
    }
    static void testPojo() throws  Exception{
        User u1 = new User();
        u1.username = "u1";
        Addr addr = new Addr();
        addr.province = "p1" ;
        addr.city = "c1";
        u1.addrs = new ArrayList<Addr>();
        u1.addrs.add(addr);

        Kryo kryo = new Kryo();
        FieldSerializer<User> fieldSerializer = new FieldSerializer<User>(kryo,User.class);
        CollectionSerializer listSerializer = new CollectionSerializer();
        fieldSerializer.getField("addrs").setSerializer(listSerializer);
        kryo.register(User.class,new FieldSerializer(kryo,User.class));
    /*    kryo.register(Addr.class,new FieldSerializer(kryo,Addr.class));
        kryo.register(ArrayList.class,new CollectionSerializer());*/
        Output output = new Output(new FileOutputStream("file.bin"));
        kryo.writeObject(output, u1);
        output.close();

        Input input = new Input(new FileInputStream("file.bin"));
        Object i = kryo.readObject(input, u1.getClass());
        input.close();
        assert i == u1;
    }
    static class User implements Serializable{
        public String username;
        public List<Addr> addrs;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            User user = (User) o;

            if (!username.equals(user.username)) return false;
            return addrs.equals(user.addrs);
        }

        @Override
        public int hashCode() {
            int result = username.hashCode();
            result = 31 * result + addrs.hashCode();
            return result;
        }
    }
    static class Addr{
        public String province;
        public String city;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Addr addr = (Addr) o;

            if (!province.equals(addr.province)) return false;
            return city.equals(addr.city);
        }

        @Override
        public int hashCode() {
            int result = province.hashCode();
            result = 31 * result + city.hashCode();
            return result;
        }
    }
    static void testByte()throws Exception{
        Byte o = new Byte("100");
        serderPrimitive(o, o);
    }
    static void testShort()throws Exception{
        Short o = new Short("100");
        serderPrimitive(o, o);
    }
    static void testInt()throws Exception{
        Integer o = new Integer(100);
        serderPrimitive(o, o);
    }
    static void testLong()throws Exception{
        Long o = new Long(100);
        serderPrimitive(o, o);
    }
    static void testFloat()throws Exception{
        Float o = new Float(100.01);
        serderPrimitive(o, o);
    }
    static void testDouble()throws Exception{
        Double o = new Double(100.01);
        serderPrimitive(o, o);
    }
    static void testString()throws Exception{
        String o = "aaabbb";
        serderPrimitive(o,o);
    }
    static void testDate()throws Exception{
        Kryo kryo = new Kryo();
        Date object = new Date();
        Output output = new Output(new FileOutputStream("file.bin"));
        kryo.writeObject(output, object);
        output.close();

        Input input = new Input(new FileInputStream("file.bin"));
        Object i = kryo.readObject(input, object.getClass());
        input.close();
        assert i == object;
    }



    public static void serderPrimitive(Object object, Object except) throws Exception {
        Kryo kryo = new Kryo();
        Output output = new Output(new FileOutputStream("file.bin"));
        kryo.writeObject(output, object);
        output.close();

        Input input = new Input(new FileInputStream("file.bin"));
        Object i = kryo.readObject(input, object.getClass());
        input.close();
        assert i == except;
    }
}