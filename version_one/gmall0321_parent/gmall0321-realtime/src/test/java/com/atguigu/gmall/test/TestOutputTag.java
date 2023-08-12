package com.atguigu.gmall.test;

import org.apache.flink.util.OutputTag;

/**
 * @author Felix
 * @date 2022/9/4
 */
public class TestOutputTag {
    public static void main(String[] args) {
        // OutputTag<String> dirtyTag = new OutputTag<>("dirtyTag");
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
    }
}
