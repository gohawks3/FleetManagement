import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.predixion.mlsmclient.mlsm.wrapper.MLSMWrapper;
import predixion.Helpers.SchemaDefinition;
import predixion.Helpers.SharedUtils;
import org.predixion.mlsmclient.interfaces.IMLSMExecuteWithCache;
import Aggregation.CacheInfo;


/**
 * This is to test that you can do prediction on IoT
 * device. The device can cache a set time period of rows
 */

/**
 * @author cxu
 * 
 */
public class FMSample {

	private MLSMWrapper mlsmWrapper;
	IMLSMExecuteWithCache mlsmExecuteWithCache;
	// set arbitrary max rows to 512
	int cacheMaxRows = 512;
	private static String testfilesFolder = "FMSampleFiles";
	private static String nestedTablesMLSMFile = testfilesFolder + System.getProperty("file.separator") + "fleet.mlsm";
	

	public FMSample(String mlsmFileName, boolean skipValidation) throws Exception {
		mlsmWrapper = MLSMWrapper.CreateWrapper();
		mlsmWrapper.SetMLSMFile(mlsmFileName);
		mlsmWrapper.SetCacheMaxRows(cacheMaxRows);
		mlsmWrapper.SetValidateMLSMSignature(skipValidation);
	}
	
	public static void main(String[] args) 
	{	
		try
		{
			String inputFile1 = testfilesFolder + System.getProperty("file.separator") + "FleetSensorData.csv";
			String inputFile2 = testfilesFolder + System.getProperty("file.separator") + "MaintenanceRecord.csv";

			testfilesFolder = System.getProperty("user.dir") + System.getProperty("file.separator") + testfilesFolder;
			String outputFile = testfilesFolder + System.getProperty("file.separator") + "NestedTableOutput.txt";
			FMSample test = new FMSample(nestedTablesMLSMFile, true);
			test.RunNestedTest(outputFile, inputFile1, inputFile2);
		}
		catch (Exception e)
		{
			System.out.println(e);
		}
	}
	
	private void RunNestedTest(String outputFile, String caseTableFile, String relatedTableFile) throws Exception {		
		// get execution engine
		mlsmExecuteWithCache = mlsmWrapper.GetAggregationEngine();
		ExecuteMLSM(outputFile, caseTableFile, relatedTableFile);
		
		// Reload MLSM - inputs much be same with the previous mlsm
		System.out.println("Running nested table test on fleet dataset using reload");
		String nestedTablesMLSMFile2 = testfilesFolder + System.getProperty("file.separator") + "fleet2_90dayWarnBrakes.mlsm";
		String mlsm2 = ReadFromFile(nestedTablesMLSMFile2);
		
		outputFile = testfilesFolder + System.getProperty("file.separator") + "NestedTableOutput2.txt";
		mlsmExecuteWithCache.ReloadMLSM(mlsm2);
		ExecuteMLSM(outputFile, caseTableFile, relatedTableFile);
		
	}
	
	private void ExecuteMLSM(String outputFile, String caseTableFile, String relatedTableFile) throws Exception
	{		
		// output schema
		ArrayList<SchemaDefinition> outputSchema = mlsmExecuteWithCache.GetOutputSchema();
		
		if (outputFile == null)
			throw new IllegalArgumentException("OutputFile cannot be empty");
		
		// delete existing file
		try
		{
			File file = new File(outputFile);
			file.delete();
		}
		catch (Exception e)
		{
			System.out.println(outputFile + "is not there!");
		}

		int idx = 0; // count of rows
		BufferedReader brRelatedTable = new BufferedReader(new FileReader(relatedTableFile));
		brRelatedTable.readLine(); // throw away header line
		BufferedReader brCaseTable = new BufferedReader(new FileReader(caseTableFile));
		brCaseTable.readLine(); // throw away header line
		
		while (true)
		{
			idx ++;
			if (idx % 30 == 0)
			{
				String caseRow;
				// input should only have data, no header since it's streamed from sensor
				caseRow = brCaseTable.readLine();
				if (caseRow == null) break;
				// input should only have data, no header since it's streamed from sensor				
				String[] caseStrline = caseRow.split(",");
				Object[] caseLine = new Object[caseStrline.length];
				for (int i = 0; i<caseStrline.length-8; i++) // the last 8 columns are target columns, should not be used as input
				{
					caseLine[i] = caseStrline[i];
				}			
				mlsmExecuteWithCache.AddRow(caseLine);
				
				// score on this line
				Iterable<Object[]> rets = mlsmExecuteWithCache.Execute();

				for (Object[] output : rets)
				{
					for (int j = 0; j < output.length; j++)
					{
						WriteToFile(outputFile, String.format("%s:\t%s", outputSchema.get(j).getColumnName(), output[j]));
					}
				}				
			}
			// input should only have data, no header since it's streamed from sensor		
			// get input data (simulate streaming)
			String relatedRow = brRelatedTable.readLine();
			if (relatedRow == null) break;
			// test only. In real world, related table/case table can't be read in whole in a file
			String[] strline = relatedRow.split(",");
			Object[] line = new Object[strline.length];
			for (int i = 0; i<strline.length; i++)
			{				
				line[i] = strline[i];
			}
		
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			Date date = null;
			if (!(strline[2].equals("")))
			{
				date = formatter.parse(strline[2]);
			}
			line[2] = date;
			mlsmExecuteWithCache.AddRow("MaintenanceRecord", line);
		}	
		System.out.println("done");
		brCaseTable.close();
		brRelatedTable.close();
	}
	
	
	public static String ReadFromFile(String fileName) throws IOException {
	    byte[]encoded = Files.readAllBytes(Paths.get(fileName)); 
	    return new String(encoded, StandardCharsets.UTF_8);
	}

	
	public static void WriteToFile(String fileName, String text) throws IOException {
		File file = new File(fileName);
		if (!file.exists())
		{
			file.createNewFile();
		}
		
		FileOutputStream fos = new FileOutputStream(file, true);		
		fos.write(text.getBytes());
		fos.write(System.getProperty("line.separator").getBytes());
		fos.flush();
		fos.close();
	}
}
