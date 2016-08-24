package banana.test.main;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import com.alibaba.fastjson.JSON;
import com.banana.master.impl.CrawlerMasterServer;

import banana.core.modle.CrawlData;
import banana.core.processor.DataProcessor;
import banana.core.protocol.CrawlerMasterProtocol;
import banana.core.protocol.DownloadProtocol;
import banana.core.protocol.Task;
import banana.crawler.dowload.impl.DownloadServer;

public class TestTask {
	
	static CrawlerMasterServer crawlerMasterServer;
	static Server masterServer;
	static Server downloadServer;

	public static void main(String[] args) throws Exception {
		CommandLineParser parser = new DefaultParser();  
		Options options = new Options();
		options.addOption("e", "extractor", true, "Set the extractor host");
		options.addOption("t", "test", true, "test task from a jsonfile");
		CommandLine commandLine = parser.parse(options, args); 
		if (!commandLine.hasOption("e") ||!commandLine.hasOption("t") ){
			System.out.println("Must have extractor configuration");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("Test", options);
			return;
		}
		if (commandLine.hasOption("t")){
			String taskFilePath = commandLine.getOptionValue('t');
			Task task = initOneTask(taskFilePath);
			task.verify();
			String extractorAddress = commandLine.getOptionValue("e");
			TestTask.start( task, extractorAddress);
			return;
		}
	}
	
	public static void start(Task task,String extractorAddress) throws Exception {
		startTestMaster(extractorAddress);
		startTestDownloader();
		System.out.println("To begin testing");
		task.thread = 1;
		if (task.queue == null){
			task.queue = new HashMap<String, Object>();
		}
		task.queue.put("delay", 1000);
		crawlerMasterServer.submitTask(task);
		while(true){
			if (crawlerMasterServer.existTask(task.name).get()){
				Thread.sleep(100);
				continue;
			}
			break;
		}
		downloadServer.stop();
		masterServer.stop();
	}

	private static void startTestDownloader() throws Exception {
		DownloadServer downloadServer = DownloadServer.initInstance("localhost:9666");
		downloadServer.dataProcessor = new DataProcessor() {
			
			public void process(List<CrawlData> objectContainer, String... args) throws Exception {
				for (CrawlData data : objectContainer){
					System.out.println(data.getData().toMap());
				}
			}
		};
		if (downloadServer != null){
			TestTask.downloadServer = new RPC.Builder(new Configuration()).setProtocol(DownloadProtocol.class)
	                .setInstance(downloadServer).setBindAddress("localhost").setPort(9777)
	                .setNumHandlers(5).build();
			TestTask.downloadServer.start();
	        downloadServer.getMasterServer().registerDownloadNode("localhost",9777);
	        System.out.println("Test Downloader started");
		}
	}

	private static void startTestMaster(String extractorAddress) throws HadoopIllegalArgumentException, IOException{
		crawlerMasterServer = new CrawlerMasterServer();
		if (crawlerMasterServer != null){
			crawlerMasterServer.setMasterPropertie("EXTRACTOR", extractorAddress);
			TestTask.masterServer = new RPC.Builder(new Configuration()).setProtocol(CrawlerMasterProtocol.class)
	                .setInstance(crawlerMasterServer).setBindAddress("localhost").setPort(9666)
	                .setNumHandlers(5).build();
			TestTask.masterServer.start();
			System.out.println("Test Master started");
		}
	}
	
	private static Task initOneTask(String taskFilePath) throws IOException {
		File file = new File(taskFilePath);
		String json = FileUtils.readFileToString(file, "utf-8");
		Task task = JSON.parseObject(json, Task.class);
		task.data = json;
		return task;
	}
}
