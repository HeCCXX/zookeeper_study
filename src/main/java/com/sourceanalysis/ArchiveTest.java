package com.sourceanalysis;

import org.apache.jute.*;

import java.io.*;
import java.util.Set;
import java.util.TreeMap;

/**
 * @ClassName ArchiveTest
 * @Description 源码分析，序列化和反序列化
 * 下面例子中，首先将序列化的数据写入test.txt中，写入的种类类型有bool、bytes、double、float、int、Record、map
 * 然后读取序列化好的文件，将其中内容反序列化
 * @Author 贺楚翔
 * @Date 2020-06-23 10:35
 * @Version 1.0
 **/
public class ArchiveTest {
    public static void main(String[] args) throws IOException {
        String path = "F://test.txt";
        final FileOutputStream out = new FileOutputStream(new File(path));
        final BinaryOutputArchive archive = BinaryOutputArchive.getArchive(out);
        archive.writeBool(true,"boolean");
        final byte[] bytes = "hcxnb".getBytes();
        archive.writeBuffer(bytes,"buffer");
        archive.writeDouble(13.14,"double");
        archive.writeFloat(5.20f,"float");
        archive.writeInt(520,"int");
        final Person person = new Person(23, "hcx");
        archive.writeRecord(person,"hcx");
        final TreeMap<String, Integer> map = new TreeMap<>();
        map.put("hcx",23);
        map.put("hgg",24);
        final Set<String> keys = map.keySet();
        archive.startMap(map,"map");
        int i = 0;
        for (String key : keys) {
            String tag = i + "";
            archive.writeString(key,tag);
            archive.writeInt(map.get(key),tag);
            i++;
        }
        archive.endMap(map,"map");

        //反序列化
        final FileInputStream inputStream = new FileInputStream(new File(path));
        final BinaryInputArchive inputArchive = BinaryInputArchive.getArchive(inputStream);

        System.out.println(inputArchive.readBool("boolean"));
        System.out.println(new String(inputArchive.readBuffer("buffer")));
        System.out.println(inputArchive.readDouble("double"));
        System.out.println(inputArchive.readFloat("float"));
        System.out.println(inputArchive.readInt("int"));
        final Person person1 = new Person();
        inputArchive.readRecord(person1,"hcx");
        System.out.println(person1);

        final Index index = inputArchive.startMap("map");
        int j = 0;
        while (!index.done()){
            String tag = j + "";
            System.out.println("key = " + inputArchive.readString(tag)+
                    ",value = " + inputArchive.readInt(tag));
            index.incr();
            j++;
        }
    }

    /**
    * @Author HCX
    * @Description //内部类，实现Record，实现序列化和反序列化方法
    * @Date 13:15 2020-06-23
    * @return
    * @exception
    **/
    private static class Person implements Record {
        private int age;
        private String name;
        public Person(int i, String hcx) {
            age = i;
            name = hcx;
        }

        public Person() {
        }

        @Override
        public void serialize(OutputArchive archive, String tag) throws IOException {
            archive.startRecord(this,tag);
            archive.writeInt(age,"age");
            archive.writeString(name,"name");
            archive.endRecord(this,tag);
        }

        @Override
        public void deserialize(InputArchive archive, String tag) throws IOException {
            archive.startRecord(tag);
            age = archive.readInt(tag);
            name = archive.readString(tag);
            archive.endRecord(tag);
        }

        @Override
        public String toString() {
            return "Person{" +
                    "age=" + age +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}
