import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import storageManager.*;

public class ScriptInterpreter {

	public static void main(String[] args) {
		
		MainMemory mem = new MainMemory();
		Disk disk = new Disk();
		SchemaManager schema_manager = new SchemaManager(mem, disk);
		
		try {
			FileWriter performance_file_writer = new FileWriter("out_performance.txt");		
			try {
				String fileName = args[0];
				File file = new File(fileName);
				FileReader fileReader = new FileReader(file);
				BufferedReader bufferedReader = new BufferedReader(fileReader);
				String line;
				String tmp_result;
				while ((line = bufferedReader.readLine()) != null) {
					System.out.println(line);
					disk.resetDiskIOs();
	    			disk.resetDiskTimer();
					StatementInterpreter.executeStmt(line, mem, disk, schema_manager);
					tmp_result = "";
					tmp_result += line + "\n";
					tmp_result += "Dish I/O: " + disk.getDiskIOs() + "\n";
					tmp_result += "Execution time: " + disk.getDiskTimer() + " ms" + "\n\n";
					performance_file_writer.write(tmp_result);
				}
				fileReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			performance_file_writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}