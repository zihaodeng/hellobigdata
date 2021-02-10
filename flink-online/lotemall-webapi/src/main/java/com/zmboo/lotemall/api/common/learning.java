package com.zmboo.lotemall.api.common;

import org.apache.logging.log4j.core.util.ArrayUtils;
import scala.Tuple2;
import sun.security.util.ArrayUtil;

import javax.swing.tree.TreeNode;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class LRUCache {

    // Cache数据 key,value {1=1, 2=2 ...}
    LinkedHashMap<Integer,Integer> map=new LinkedHashMap<Integer, Integer>();
    // Cache数据的使用次数 key,num
    //HashMap<Integer,Integer> mapLevel = new HashMap<>();
    //Map<Integer, Integer> mapLevel = new HashMap<Integer, Integer>();
    LinkedHashMap<Integer,Integer> mapUse=new LinkedHashMap<Integer, Integer>();
    // 记录使用次数 key,num
    //HashSet<Tuple2<Integer,Integer>> setTpMin = new HashSet<Tuple2<Integer,Integer>>();;
    int minKey=0;
    //hash key的个数
    int count =0;

    public LRUCache(int capacity) {
        this.count=capacity;
    }

    public int get(int key) {
        //更新使用记录mapUse
//        if(mapLevel.containsKey(key)){
//            num = mapLevel.get(key);
//            mapLevel.put(key,num+1);
//        }
        //返回值
        if(!map.containsKey(key)){
            return -1;
        }else {
            UpdUseMap(key);
            return map.get(key);
        }
    }

    public void put(int key, int value) {

        if(map.size()<count){
            //map未满
            map.put(key, value);
            UpdUseMap(key);
        }else{
            //map已满
            //找到使用最少的key 移除使用最少的key
            //如果mapLevel为空，则移除map中第一个元素
//            if(mapLevel.size()==0){
//                Integer keyFirst=GetMapFirstKey();
//                map.remove(keyFirst);
//                mapLevel.remove(keyFirst);
//            }else{
//                //移除使用次数最少的元素
//                //Integer key=tpMin._1();
//                Integer keyMin= GetMinKey();
//                map.remove(keyMin);
//                mapLevel.remove(keyMin);
//            }
//            //末尾添加
//            map.put(key, value);
//            if(!mapLevel.containsKey(key)) {
//                mapLevel.put(key, 0);
//            }
            //如果是已存在的值
            if(map.containsKey(key)){
                //更新map
                map.put(key,value);
                //更新mapUse
                UpdUseMap(key);
            }else {
                //获得mapUse最旧的key
                Integer firstKey = GetMapFirstKey(mapUse);
                // 移除最旧的key
                map.remove(firstKey);
                // map 添加新值
                map.put(key, value);
                // 移除最旧的key
                mapUse.remove(firstKey);
                // 更新最新的key
                UpdUseMap(key);
            }
        }

    }

    private void UpdUseMap(int key){
        if(mapUse.containsKey(key)){
            mapUse.remove(key);
        }
        mapUse.put(key,1);
    }

//    private Integer GetMinKey(){
//        Integer key=null;
//        Integer min=null;
//        Iterator<Map.Entry<Integer,Integer>> it =mapLevel.entrySet().iterator();
//        while(it.hasNext()){
//            Map.Entry<Integer,Integer> entry = it.next();
//            if(min==null){
//                key=entry.getKey();
//                min=entry.getValue();
//            }else{
//                if(entry.getValue()<min){
//                    key=entry.getKey();
//                    min=entry.getValue();
//                }
//            }
//        }
//        return key;
//    }

    private Integer GetMapFirstKey(Map<Integer,Integer> map){
        Integer firstKey=0;
        Iterator<Map.Entry<Integer,Integer>> it =map.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry<Integer,Integer> entry = it.next();
            firstKey=entry.getKey();
            break;
        }
        return firstKey;
    }
}

public class learning {
//    public static void main(String[] args) {
//        LRUCache lRUCache = new LRUCache(2);
//        lRUCache.get(2);
//        lRUCache.put(2, 6);
//        lRUCache.get(1);
//        lRUCache.put(1, 5);
//        lRUCache.put(1, 2);
//        lRUCache.get(1);
//        lRUCache.get(2);

//        lRUCache.put(1, 1); // 缓存是 {1=1}
//        lRUCache.put(2, 2); // 缓存是 {1=1, 2=2}
//        lRUCache.get(1);    // 返回 1
//        lRUCache.put(3, 3); // 该操作会使得关键字 2 作废，缓存是 {1=1, 3=3}
//        lRUCache.get(2);    // 返回 -1 (未找到)
//        lRUCache.put(4, 4); // 该操作会使得关键字 1 作废，缓存是 {4=4, 3=3}
//        lRUCache.get(1);    // 返回 -1 (未找到)
//        lRUCache.get(3);    // 返回 3
//        lRUCache.get(4);    // 返回 4

//    }

    public void test() {

        LinkedHashMap<Integer, Integer> map = new LinkedHashMap<Integer, Integer>();
        map.size();
        Iterator it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            entry.getKey();
            entry.getValue();
        }
        //map.containsKey()

        Tuple2<Integer, Integer> tpMin = new Tuple2<Integer, Integer>(1, 2);
        Integer key = tpMin._1();

        HashSet<Integer> set = new HashSet<Integer>();
        set.add(1);

        Map<Integer, Integer> mapLevel = new HashMap<Integer, Integer>();
        //mapLevel.containsKey()

        List<Integer> lst = new ArrayList();
        lst.add(1);
        lst.size();
        lst.get(0);
        Iterator<Integer> it1 = lst.iterator();
        while (it1.hasNext()) {
            Integer val = it1.next();
        }
    }

    public int trap2(int[] height) {
        //List<Integer> lstArr;
        //lstArr.size()
        List<Integer> lstPre = new ArrayList();

        lstPre.toArray(new Integer[0]);
        List<Integer[]> lst = new ArrayList<Integer[]>();
        Integer cnt = 0;
        return cnt;
    }

    /**
     * 接雨水
     */
    int number = 0;

    public int trap(int[] height) {
        List<Integer> lstHeight = toList(height);
        clacNum(lstHeight);
        return number;
    }

    private List<Integer> toList(int[] arr) {
        List<Integer> lst = new ArrayList<Integer>();
        for (int item : arr) {
            lst.add(item);
        }

        System.out.println();
        return lst;
    }

    public void gettp(type tp) {

    }

    public enum type {
        left, middle, right
    }

    private void clacNum(List<Integer> lstHeight) {

        List<Integer> lstMax = selMax(lstHeight);//length=2
        List<List<Integer>> lstArr = splitArray(lstHeight, lstMax.get(0));
        if (lstArr.size() <= 0) return;
        //Left
        List<Integer> lstLeftMax = selLeftMax(lstArr.get(0));
        number += (lstMax.get(0) - lstLeftMax.get(0) - 1) * lstLeftMax.get(1);
        int tmpLeft = 0;
        for (int i = lstLeftMax.get(0) + 1; i < lstMax.get(0); i++) {
            tmpLeft += lstHeight.get(i);
        }
        number -= tmpLeft;
        //右
        List<Integer> lstRightMax = selRightMax(lstArr.get(1));
        number = (lstRightMax.get(0) - lstMax.get(0) - 1) * lstRightMax.get(1);
        int tmpRight = 0;
        for (int j = lstMax.get(0) + 1; j < lstRightMax.get(0); j++) {
            tmpRight += lstHeight.get(j);
        }
        number -= tmpRight;

        if (lstArr.size() > 0) {
            clacNum(lstArr.get(0));
            clacNum(lstArr.get(1));
        }
    }

    //拆分数组 一分为二
    private List<List<Integer>> splitArray(List<Integer> arr, Integer index) {
        List<List<Integer>> lst = new ArrayList();
        if (arr.size() <= 1) return lst;

        List<Integer> lstPre = new ArrayList();
        List<Integer> lstLast = new ArrayList();
        for (int i = 0; i <= arr.size(); i++) {
            if (i < index) {
                lstPre.add(arr.get(i));
            } else if (i > index) {
                lstLast.add(arr.get(i));
            } else {

            }
        }
        lst.add(lstPre);
        lst.add(lstLast);
        return lst;
    }

    //查找数组中的最大值位置和最大值
    private List<Integer> selMax(List<Integer> arr) {
        int max = 0;
        int maxIndex = 0;
        for (int i = 0; i <= arr.size(); i++) {
            if (arr.get(i) > max) {
                max = arr.get(i);
                maxIndex = i;
            }
        }

        List<Integer> lstMax = new ArrayList();
        lstMax.add(maxIndex);
        lstMax.add(max);
        return lstMax;
    }

    private List<Integer> selLeftMax(List<Integer> arr) {
        int max = 0;
        int maxIndex = 0;
        for (int i = arr.size() - 2; i >= 0; i--) {
            if (arr.get(i) > max) {
                max = arr.get(i);
                maxIndex = i;
            }
        }

        List<Integer> lstMax = new ArrayList();
        lstMax.add(maxIndex);
        lstMax.add(max);
        return lstMax;
    }

    private List<Integer> selRightMax(List<Integer> arr) {
        int max = 0;
        int maxIndex = 0;
        for (int i = 1; i <= arr.size(); i++) {
            if (arr.get(i) > max) {
                max = arr.get(i);
                maxIndex = i;
            }
        }

        List<Integer> lstMax = new ArrayList();
        lstMax.add(maxIndex);
        lstMax.add(max);
        return lstMax;
    }

    /**
     *
     */
    public class ListNode {
        int val;
        ListNode next;

        ListNode(int x) {
            val = x;
        }
    }

    public ListNode reverseList(ListNode head) {
        do {

        } while (true);
    }

    public static List<Integer[]> clac(List<Integer[]> lst) {
        List<Integer[]> lstResult = new ArrayList<Integer[]>();
        for (Integer i = 0; i < lst.size(); i++) {
            Integer[] xy = lst.get(i);
            Integer x = xy[0];
            Integer y = xy[1];
            Boolean bflg = true;//默认是
            for (Integer j = 0; j < lst.size(); j++) {
                if(i.equals(j)) continue;;
                Integer[] c_xy = lst.get(j);
                Integer c_x = c_xy[0];
                Integer c_y = c_xy[1];
                //如果有一个>x and >y 则失败 break;
                //如果不存在则 成功 记录该值到返回结果
                if (c_x.compareTo(x) >= 0 && c_y.compareTo(y) >= 0) {
                    bflg = false;
                    break;
                }
            }
            if (bflg) {
                lstResult.add(xy);
            }
        }
        return lstResult;
    }

    public static void main(String[] args) {
        List<Integer[]> lst = new ArrayList<Integer[]>();
        lst.add(new Integer[]{5,0});
        lst.add(new Integer[]{1,2});
        lst.add(new Integer[]{5,3});
        lst.add(new Integer[]{4,6});
        lst.add(new Integer[]{7,5});
        lst.add(new Integer[]{9,0});
        System.out.println(clac(lst));
        Lock lock= new ReentrantLock();

        List<String> list= new CopyOnWriteArrayList<String>();

    }

}


