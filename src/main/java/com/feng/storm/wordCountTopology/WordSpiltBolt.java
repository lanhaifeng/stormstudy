package com.feng.storm.wordCountTopology;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * stormstudy
 * 2020/3/16 17:59
 * 处理数据，进行分组
 *
 * @author lanhaifeng
 * @since
 **/
public class WordSpiltBolt extends BaseBasicBolt {

	public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
		String msg = tuple.getStringByField("word");
		String[] words = msg.toLowerCase().split(" ");
		for (String word : words) {
			basicOutputCollector.emit(new Values(word));//向下一个bolt发射数据
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("count"));
	}
}
