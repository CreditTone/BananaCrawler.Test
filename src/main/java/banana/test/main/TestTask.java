package banana.test.main;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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

import banana.core.download.impl.DefaultPageDownloader;
import banana.core.modle.CrawlData;
import banana.core.processor.DataProcessor;
import banana.core.processor.PageProcessor;
import banana.core.protocol.CrawlerMasterProtocol;
import banana.core.protocol.DownloadProtocol;
import banana.core.protocol.Extractor;
import banana.core.protocol.Task;
import banana.core.request.HttpRequest;
import banana.core.request.StartContext;
import banana.core.response.Page;
import banana.crawler.dowload.impl.DownloadServer;
import banana.crawler.dowload.impl.JsonRpcExtractor;
import banana.crawler.dowload.processor.JSONConfigPageProcessor;
import banana.master.impl.CrawlerMasterServer;
import banana.master.task.TaskTracker;

public class TestTask {

	static CrawlerMasterServer crawlerMasterServer;
	static Server masterServer;
	static Server downloadServer;

	public static void main(String[] args) throws Exception {
		CommandLineParser parser = new DefaultParser();
		Options options = new Options();
		options.addOption("e", "extractor", true, "Set the extractor host");
		options.addOption("t", "test", true, "Test task from a jsonfile");
		options.addOption("p", "processor", true, "Test the target processor");
		options.addOption("v", "v", false, "print detail info");
		CommandLine commandLine = parser.parse(options, args);
		if (!commandLine.hasOption("e") || !commandLine.hasOption("t")) {
			System.out.println("Must have extractor configuration");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("Test", options);
			return;
		}
		String taskFilePath = commandLine.getOptionValue('t');
		Task task = initOneTask(taskFilePath);
		String extractorAddress = commandLine.getOptionValue("e");
		if (commandLine.hasOption("p")){
			String[] split = commandLine.getOptionValue('p').split(",");
			String processorIndex = split[0];
			String url = split[1];
			String taskid = task.name + "_" + new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
			Task.Processor processor = null;
			for (Task.Processor p : task.processors) {
				if(p.getIndex().equals(processorIndex)){
					processor = p;
					break;
				}
			}
			Extractor extractor = new JsonRpcExtractor(extractorAddress);
			JSONConfigPageProcessor pro = new JSONConfigPageProcessor(taskid, processor, extractor);
			testProcessor(pro, url,commandLine.hasOption("v"));
		}else{
			task.verify();
			TestTask.start(task, extractorAddress);
		}
	}

	public static void start(Task task, String extractorAddress) throws Exception {
		startTestMaster(extractorAddress);
		startTestDownloader();
		System.out.println("To begin testing");
		task.thread = 1;
		if (task.queue == null) {
			task.queue = new HashMap<String, Object>();
		}
		task.queue.put("delay", 1000);
		crawlerMasterServer.submitTask(task);
		while (true) {
			if (crawlerMasterServer.existTask(task.name).get()) {
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
				for (CrawlData data : objectContainer) {
					System.out.println(data.getData().toMap());
				}
			}
		};
		if (downloadServer != null) {
			TestTask.downloadServer = new RPC.Builder(new Configuration()).setProtocol(DownloadProtocol.class)
					.setInstance(downloadServer).setBindAddress("localhost").setPort(9777).setNumHandlers(5).build();
			TestTask.downloadServer.start();
			downloadServer.getMasterServer().registerDownloadNode("localhost", 9777);
			System.out.println("Test Downloader started");
		}
	}

	private static void startTestMaster(String extractorAddress) throws HadoopIllegalArgumentException, IOException {
		crawlerMasterServer = new CrawlerMasterServer();
		if (crawlerMasterServer != null) {
			crawlerMasterServer.setMasterPropertie("EXTRACTOR", extractorAddress);
			TestTask.masterServer = new RPC.Builder(new Configuration()).setProtocol(CrawlerMasterProtocol.class)
					.setInstance(crawlerMasterServer).setBindAddress("localhost").setPort(9666).setNumHandlers(5)
					.build();
			TaskTracker.MODE = TaskTracker.TEST_MODE;
			TestTask.masterServer.start();
			System.out.println("Test Master started");
		}
	}
	
	private static void testProcessor(PageProcessor pro,String url,boolean detail) throws Exception{
		StartContext startContext = new StartContext();
		DefaultPageDownloader downloader = new DefaultPageDownloader();
		downloader.open();
		Page page = downloader.go(startContext.createPageRequest(url, ""));
		List<HttpRequest> queue = new ArrayList<HttpRequest>();
		List<CrawlData> objectContainer = new ArrayList<CrawlData>();
		pro.process(page, startContext, queue, objectContainer);
		System.out.println("extract request:" + queue.size());
		if (detail){
			for (HttpRequest request : queue) {
				System.out.print(request.getUrl());
				System.out.print(" attr");
				System.out.println(request.getAttributes());
			}
		}
		System.out.println("extract crawler_data:" + objectContainer.size());
		if (detail){
			for (CrawlData data : objectContainer) {
				System.out.println(data.getData().toMap());
			}
		}
		downloader.close();
	}

	private static Task initOneTask(String taskFilePath) throws IOException {
		File file = new File(taskFilePath);
		String json = FileUtils.readFileToString(file, "utf-8");
		Task task = JSON.parseObject(json, Task.class);
		task.data = json;
		return task;
	}

}
