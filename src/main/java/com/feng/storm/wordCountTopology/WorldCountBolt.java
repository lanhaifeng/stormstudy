package com.feng.storm.wordCountTopology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import lombok.extern.log4j.Log4j;

import java.util.HashMap;
import java.util.Map;

/**
 * stormstudy
 * 2020/3/16 18:00
 * 单词统计bolt
 *
 * @author lanhaifeng
 * @since
 **/
@Log4j
public class WorldCountBolt extends BaseRichBolt {

	/**
	 * 保存单词和对应的计数
	 */
	private HashMap<String, Integer> counts = null;

	private long count = 1;

	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.counts = new HashMap<String, Integer>();
	}

	public void execute(Tuple tuple) {
		String msg = tuple.getStringByField("count");
		if("_deactivate".equals(tuple.getSourceStreamId()) && "shutDown".equals(msg)) {
			cleanup();
		}
		log.info("第" + count + "次统计单词出现的次数");
		System.out.println("第" + count + "次统计单词出现的次数");
		/**
		 * 如果不包含该单词，说明在该map是第一次出现
		 * 否则进行加1
		 */
		if (!counts.containsKey(msg)) {
			counts.put(msg, 1);
		} else {
			counts.put(msg, counts.get(msg) + 1);
		}
		count++;
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}

	@Override
	public void cleanup() {
		log.info("===========开始显示单词数量============");
		for (Map.Entry<String, Integer> entry : counts.entrySet()) {
			System.out.println("输入统计结果：" + entry.getKey() + ": " + entry.getValue());
		}
		log.info("===========结束============");
	}
}
