package com.feng.storm.wordCountTopology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import lombok.extern.log4j.Log4j;

import java.util.Map;

/**
 * stormstudy
 * 2020/3/16 17:52
 * 发射数据流
 *
 * @author lanhaifeng
 * @since
 **/
@Log4j
public class WordSpout extends BaseRichSpout {

	private SpoutOutputCollector spoutOutputCollector;
	private int count = 1;
	private String[] messages = {
			"My nickname is test",
			"My blog address is http://www.test.com/",
			"My interest is playing games"
	};

	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		this.spoutOutputCollector = spoutOutputCollector;
	}

	public void nextTuple() {
		if (count <= messages.length && count > 1 && count < Integer.MAX_VALUE) {
			log.info("第" + count + "次开始发送数据...");
			this.spoutOutputCollector.emit(new Values(messages[count - 1]));
		}
		count++;
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("word"));
	}

	@Override
	public void deactivate() {
		spoutOutputCollector.emit("_deactivate", new Values("shutDown"));
	}

}
