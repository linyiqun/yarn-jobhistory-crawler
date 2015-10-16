package mytool;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class Main {
	public static void main(String[] args) {
		int writedb;
		YarnClient client;
		client = YarnClient.createYarnClient();
		client.init(new Configuration());
		client.start();
		
		writedb = 0;
		if (args.length == 1) {
			if (args[0].equals("writedb")) {
				writedb = 1;
			}
		} else {
			System.out.println("param num is error");
			return;
		}
		
		try {
			printNodeStatus(client, writedb);
		} catch (YarnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			client.stop();
			System.out.println("关闭客户端");
		}
	}

	/**
	 * Prints the node report for node id.
	 * 
	 * @param nodeIdStr
	 * @throws YarnException
	 */
	private static void printNodeStatus(YarnClient client, int writedb)
			throws YarnException, IOException {
		String time;
		String nodelables;
		int memUsed;
		int memAvail;
		int vcoresUsed;
		int vcoresAvail;
		String[] values;
		DbClient dbClient;
		List<NodeReport> nodesReport = client.getNodeReports();
		// Use PrintWriter.println, which uses correct platform line ending.
		NodeReport nodeReport = null;
		System.out.println("node num is " + nodesReport.size()
				+ ", writedb is " + writedb);

		dbClient = null;
		if (writedb == 1) {
			dbClient = new DbClient(BaseValues.DB_URL, BaseValues.DB_USER_NAME,
					BaseValues.DB_PASSWORD,
					BaseValues.DB_NM_REPORT_STAT_TABLE_NAME);

			dbClient.createConnection();
		}

		for (NodeReport report : nodesReport) {
			nodeReport = report;
			values = new String[BaseValues.DB_COLUMN_NM_REPORT_LEN];

			System.out.println("Node Report : ");
			System.out.print("\tNode-Id : ");
			System.out.println(nodeReport.getNodeId());
			values[BaseValues.DB_COLUMN_NM_REPORT_NODE_NAME] = nodeReport
					.getNodeId().getHost();

			System.out.print("\tRack : ");
			System.out.println(nodeReport.getRackName());
			values[BaseValues.DB_COLUMN_NM_REPORT_RACK] = nodeReport
					.getRackName();

			System.out.print("\tNode-State : ");
			System.out.println(nodeReport.getNodeState());
			values[BaseValues.DB_COLUMN_NM_REPORT_NODE_STATE] = nodeReport
					.getNodeState().toString();

			System.out.print("\tNode-Http-Address : ");
			System.out.println(nodeReport.getHttpAddress());
			values[BaseValues.DB_COLUMN_NM_REPORT_NODE_HTTP_ADDRESS] = nodeReport
					.getHttpAddress();

			System.out.print("\tLast-Health-Update : ");
			time = DateFormatUtils.format(
					new Date(nodeReport.getLastHealthReportTime()),
					"yyyy-MM-dd HH:mm:ss");
			System.out.println(time);
			values[BaseValues.DB_COLUMN_NM_REPORT_LAST_HEALTH_UPDATE] = time;
			values[BaseValues.DB_COLUMN_NM_REPORT_TIME] = getCurrentTime();

			System.out.print("\tHealth-Report : ");
			System.out.println(nodeReport.getHealthReport());
			values[BaseValues.DB_COLUMN_NM_REPORT_HEALTH_REPORT] = nodeReport
					.getHealthReport();

			System.out.print("\tContainers : ");
			System.out.println(nodeReport.getNumContainers());
			values[BaseValues.DB_COLUMN_NM_REPORT_CONTAINERS] = String
					.valueOf(nodeReport.getNumContainers());

			System.out.print("\tMemory-Used : ");
			memUsed = (nodeReport.getUsed() == null) ? 0 : (nodeReport
					.getUsed().getMemory());
			System.out.println(memUsed + "MB");
			values[BaseValues.DB_COLUMN_NM_REPORT_MEMORY_USED] = String
					.valueOf(memUsed);

			System.out.print("\tMemory-Capacity : ");
			System.out.println(nodeReport.getCapability().getMemory() + "MB");
			memAvail = nodeReport.getCapability().getMemory() - memUsed;
			values[BaseValues.DB_COLUMN_NM_REPORT_MEMORY_AVAIL] = String
					.valueOf(memAvail);

			System.out.print("\tCPU-Used : ");
			vcoresUsed = (nodeReport.getUsed() == null) ? 0 : (nodeReport
					.getUsed().getVirtualCores());
			System.out.println(vcoresUsed + " vcores");
			values[BaseValues.DB_COLUMN_NM_REPORT_VCORES_USED] = String
					.valueOf(vcoresUsed);

			System.out.print("\tCPU-Capacity : ");
			System.out.println(nodeReport.getCapability().getVirtualCores()
					+ " vcores");
			vcoresAvail = nodeReport.getCapability().getVirtualCores()
					- vcoresUsed;
			values[BaseValues.DB_COLUMN_NM_REPORT_VCORES_AVAIL] = String
					.valueOf(vcoresAvail);

			System.out.print("\tNode-Labels : ");
			// Create a List for node labels since we need it get sorted
			List<String> nodeLabelsList = new ArrayList<String>(
					report.getNodeLabels());
			Collections.sort(nodeLabelsList);
			nodelables = StringUtils.join(nodeLabelsList.iterator(), ',');
			System.out.println(nodelables);
			values[BaseValues.DB_COLUMN_NM_REPORT_NODE_LABELS] = nodelables;

			if (dbClient != null) {
				dbClient.insertNMReportData(values);
			}
		}

		if (nodeReport == null) {
			System.out.print("Could not find the node report for node id");
		}

		if (dbClient != null) {
			dbClient.closeConnection();
		}
	}
	
	private static String getCurrentTime() {
		Calendar calendar = Calendar.getInstance();
		Date date = calendar.getTime();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		return df.format(date);
	}
}
