package com.lorettax.StreamExtractionEngine.console.commands;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import com.lorettax.StreamExtractionEngine.core.LinkStreamFQLConfig;
import com.lorettax.StreamExtractionEngine.core.LinkStreamFQLExecutePipe;
import com.lorettax.StreamExtractionEngine.tools.Executors;
import com.lorettax.StreamExtractionEngine.tools.KafkaReader;
import com.lorettax.StreamExtractionEngine.tools.KafkaWriter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@ShellComponent
public class ConsoleCommands {
	
	private static final Logger logger = LoggerFactory.getLogger(ConsoleCommands.class);
	
	private static final String ERROR_FORMAT = "ERROR: %s";
	private static final String WARN_FORMAT = "WARN: %s";
	private static final String INFO_FORMAT = "INFO: %s";
	
	@ShellMethod("indicate starting configuration for an application")
	public String configApplication(String application) {
		ConsoleStatus consoleStatus = ConsoleStatus.getInstance();
		consoleStatus.setApplication(application);
		JSONObject applicationConfigs = consoleStatus.getApplicationConfigs();
		applicationConfigs.putIfAbsent(application, consoleStatus.newDefaultApplicationConfig());
		return String.format("start configuration for a application[%s]", application);
	}
	
	@ShellMethod("set source")
	public String setSource(String source) {
		ConsoleStatus consoleStatus = ConsoleStatus.getInstance();
		
		consoleStatus.setSource(source);
		
		return String.format(INFO_FORMAT, String.format("set source[%s]", source));
	}
	
	
	
	@ShellMethod("set sink")
	public String setSink(String sink) {
		ConsoleStatus consoleStatus = ConsoleStatus.getInstance();
		consoleStatus.setSink(sink);
		
		return String.format(INFO_FORMAT, String.format("set sink[%s]", sink));
	}
	
	
	@ShellMethod("add a field")
	public String addField(String eventType, String fieldName, String fieldPath) {
		ConsoleStatus consoleStatus = ConsoleStatus.getInstance();
		
		String application = consoleStatus.getApplication();
		if (StringUtils.isBlank(application)) {
			return String.format(ERROR_FORMAT, String.format("specify no application[%s]", application));
		}
		
		JSONObject appConfig = consoleStatus.getApplicationConfigs().getJSONObject(application);
		JSONObject fields = appConfig.getJSONObject("fields");
		fields.putIfAbsent(eventType, new JSONObject());
		JSONObject fieldsForType = fields.getJSONObject(eventType);
		JSONArray newFieldArray = new JSONArray();
		JSONObject fieldDefinition = new JSONObject();
		fieldDefinition.put("field", fieldPath);
		newFieldArray.add(fieldDefinition);
		fieldsForType.put(fieldName, newFieldArray);
		
		return String.format(INFO_FORMAT, String.format(
				"application[%s] add field, eventType[%s], fieldName[%s], fieldPath[%s]",
				application, eventType, fieldName, fieldPath));
	}
	
	
	@ShellMethod("add a feature")
	public String addFeature(String eventType, String mode, String feature) {
		ConsoleStatus consoleStatus = ConsoleStatus.getInstance();
		
		String application = consoleStatus.getApplication();
		if (StringUtils.isBlank(application)) {
			return String.format(ERROR_FORMAT, String.format("specify no application[%s]", application));
		}
		
		JSONObject appConfigs = consoleStatus.getApplicationConfigs().getJSONObject(application);
		appConfigs.putIfAbsent("features", new JSONObject());
		JSONObject features = appConfigs.getJSONObject("features");
		features.putIfAbsent(eventType, new JSONObject());
		JSONObject featuresForType = features.getJSONObject(eventType);
		featuresForType.putIfAbsent(mode, new JSONArray());
		JSONArray featuresArray = featuresForType.getJSONArray(mode);
		if (!featuresArray.contains(feature)) {
			featuresArray.add(feature);
		}
		
		return String.format(INFO_FORMAT, String.format(
				"application[%s] add feature, eventType[%s], mode[%s], feature[%s]",
				application, eventType, mode, feature));
		
	}
	
	
	@ShellMethod("add a macro")
	public String addMacro(String name, String replace) {
		ConsoleStatus consoleStatus = ConsoleStatus.getInstance();
		
		String application = consoleStatus.getApplication();
		if (StringUtils.isBlank(application)) {
			return String.format(ERROR_FORMAT, String.format("specify no application[%s]", application));
		}
		
		JSONObject appConfigs = consoleStatus.getApplicationConfigs().getJSONObject(application);
		JSONObject applicationSetting = appConfigs.getJSONObject("setting");
		applicationSetting.putIfAbsent("macros", new JSONObject());
		applicationSetting.getJSONObject("macros").put(name, replace);
		
		return String.format(INFO_FORMAT, String.format(
				"application[%s] add macro, name[%s], replace[%s]",
				application, name, replace));
		
	}
	
	
	@ShellMethod("activate configApplication")
	public String activate() {
		LinkStreamFQLConfig.loadConfig(ConsoleStatus.getInstance().getApplicationConfigs());
		return String.format(INFO_FORMAT, "activate configApplication");
	}
	
	
	@ShellMethod("list configApplication")
	public String list() {
		return String.format(INFO_FORMAT, String.format(
				"shell status configApplication[%s], stream feature extractor configApplication[%s]",
				ConsoleStatus.getInstance().getApplicationConfigs(),
				LinkStreamFQLConfig.getConfig()));
	}
	
	

	private static class WorkerRunnable implements Runnable {
		private static final Logger logger = LoggerFactory.getLogger(WorkerRunnable.class);
		
		private ConsoleStatus consoleStatus = null;
		private KafkaReader kafkaReader = null;
		private KafkaWriter kafkaWriter = null;
		LinkStreamFQLExecutePipe linkStreamFQLExecutePipe = null;
		
		private static final ExecutorService senderExecutor = Executors.createMultiQueueThreadPool(
				"sender", 1, 2,2,4096,5L);
				
		public WorkerRunnable() {
			this.consoleStatus = ConsoleStatus.getInstance();
			this.kafkaReader = createKafkaReceiver();
			this.kafkaWriter = createKafkaSender();
			this.linkStreamFQLExecutePipe = new LinkStreamFQLExecutePipe();
		}
		
		private KafkaReader createKafkaReceiver() {
			if (StringUtils.isBlank(consoleStatus.getSource())) {
				throw new IllegalArgumentException("source not configApplication");
			}
			
			KafkaSource kafkaSource = JSONObject.parseObject(consoleStatus.getSource(), KafkaSource.class);
			final Properties props = new Properties();
			props.setProperty("zookeeper.connect", kafkaSource.getZookeeper());
			props.setProperty("group.id", kafkaSource.getGroup());
			props.setProperty("auto.offset.reset", kafkaSource.getOffset());
			return new KafkaReader(props, kafkaSource.getTopic());
		}
		
		private KafkaWriter createKafkaSender() {
			if (StringUtils.isBlank(consoleStatus.getSource())) {
				throw new IllegalArgumentException("source not configApplication");
			}
			
			KafkaSink kafkaSink =
					JSONObject.parseObject(consoleStatus.getSink(), KafkaSink.class);
			final Properties props = new Properties();
			props.setProperty("metadata.broker.list", kafkaSink.getBroker());
			return new KafkaWriter(props, kafkaSink.getTopic());
		}
		
		private void send(JSONObject event) {
			kafkaWriter.send(event.toJSONString().getBytes(Charsets.UTF_8));
		}
		
		
		@Override
		public void run() {
			
			while(!consoleStatus.isStop() && !(Thread.currentThread().isInterrupted())) {
				try {
					if (kafkaReader.hasNext()) {
						final byte[] event = kafkaReader.next();
						linkStreamFQLExecutePipe
								.processAsync(event)
								.thenAcceptAsync(this::send, senderExecutor);
					} else {
						TimeUnit.MILLISECONDS.sleep(100);
					}
				} catch(InterruptedException e){
					logger.warn("Worker has been interrupted.", e);
					break;
				} catch(Exception e) {
					logger.error("exception caught", e);
				}
			}
			kafkaReader.close();
			kafkaWriter.close();
			logger.info("Worker stopped, exit");
		}
		
	}
	
	@ShellMethod("start stream feature extractor")
	public String start() {
		ConsoleStatus consoleStatus = ConsoleStatus.getInstance();
		consoleStatus.setStop(false);
		Thread worker = new Thread(new WorkerRunnable());
		consoleStatus.setWorker(worker);
		worker.start();
		return String.format(INFO_FORMAT, "start stream feature extractor");
	}
	
	@ShellMethod("stop stream feature extractor")
	public String stop() {
		ConsoleStatus consoleStatus = ConsoleStatus.getInstance();
		consoleStatus.setStop(true);
		consoleStatus.getWorker().interrupt();
		try {
			consoleStatus.getWorker().join();
		} catch (InterruptedException e) {
			logger.warn("INterruptedException caught, exit");
		}
		consoleStatus.setWorker(null);
		return String.format(INFO_FORMAT, "stop stream feature extractor");
	}
	
	
	
}