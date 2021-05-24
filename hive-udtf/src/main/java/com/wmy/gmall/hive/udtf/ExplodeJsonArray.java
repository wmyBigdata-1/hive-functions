package com.wmy.gmall.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * ClassName:ExplodeJsonArray
 * Package:com.wmy.gmall.hive.udtf
 *
 * @date:2021/5/24 14:38
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * Description: hive udtf的函数的使用
 */
public class ExplodeJsonArray extends GenericUDTF {
    @Override
    public void close() {

    }

    /**
     * 这个方法过时了，所以需要更改兼容性问题
     * 1、校验输入的参数
     * 2、
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<? extends StructField> allStructFieldRefs = argOIs.getAllStructFieldRefs();// 字段的引用

        if (allStructFieldRefs.size() != 1) {
            // 传入参数是不符合的
            throw new UDFArgumentLengthException("explode json array 函数只能接受一个参数");
        }

        // todo 参数的类型
        ObjectInspector fieldObjectInspector = allStructFieldRefs.get(0).getFieldObjectInspector();
        // 基础类型，复杂类型
        String typeName = fieldObjectInspector.getTypeName();

        if (!"string".equals(typeName)){
            throw new UDFArgumentLengthException("explode json array 函数只能接受一个string类型的参数");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("col1");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        // todo 1、获取传入的数据
        String jsonArray = objects[0].toString();

        // todo 2、将String转换为json数组
        JSONArray actions = new JSONArray(jsonArray);

        // todo 3、循环一次，取出数组中的一个json，并写出
        for (int i = 0; i < actions.length(); i++) {
            String[] result = new String[1];
            result[0] = actions.getString(i);
            forward(result);
        }
    }
}
