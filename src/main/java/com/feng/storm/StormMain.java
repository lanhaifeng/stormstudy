package com.feng.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.feng.storm.wordCountTopology.WordSpiltBolt;
import com.feng.storm.wordCountTopology.WordSpout;
import com.feng.storm.wordCountTopology.WorldCountBolt;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang.exception.ExceptionUtils;

/**
 * stormstudy
 * 2020/3/16 17:44
 * storm入口类
 *
 * @author lanhaifeng
 * @since
 **/
@Log4j
public class StormMain {

	public static void main(String[] args) {
		try {
			TopologyBuilder builder = new TopologyBuilder();
			Config conf = new Config();
			//远程集群
			if (args != null && args.length > 0) {
				log.info("运行远程模式");
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} else {//本地调试模式
				log.info("start word count");

				// 设置1个Executeor(线程)，默认一个
				builder.setSpout("word_spout", new WordSpout(), 1);
				// shuffleGrouping:表示是随机分组
				builder.setBolt("word_spit_bolt", new WordSpiltBolt(), 1).setNumTasks(1).fieldsGrouping("word_spout", new Fields("word"));
				builder.setBolt("word_count_bolt", new WorldCountBolt(), 1).setNumTasks(1).fieldsGrouping("word_spit_bolt", new Fields("count"));

				//设置一个应答者
				conf.setNumAckers(1);
				//设置一个work
				conf.setNumWorkers(1);

				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("word_count_topology", conf, builder.createTopology());

				//暂停10s，关掉任务，否则会一直运行
				Utils.sleep(180000);
				cluster.killTopology("word_count_topology");
				cluster.shutdown();
			}
		} catch (Exception e) {
			log.error("启动storm任务失败，错误：" + ExceptionUtils.getFullStackTrace(e));
		}
	}
}
