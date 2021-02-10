package udf;

import org.apache.flink.table.functions.ScalarFunction;

public class ExchangeGoodsName extends ScalarFunction {

    private  static String[] goodsName=new String[]{"蛋糕","鲜花","生鲜"};

    public  String eval(Integer goodsId){
        return goodsName[goodsId];
    }
}
