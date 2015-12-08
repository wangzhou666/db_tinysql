import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Console;
import storageManager.*;

public class ScriptInterpreter {

	public static void main(String[] args) {

		MainMemory mem = new MainMemory();
		Disk disk = new Disk();
		SchemaManager schema_manager = new SchemaManager(mem, disk);

		if (args.length == 0) {
			System.out.println("Legal commands:\n\trun statement: <Tiny-SQL statement>\n\trun script: run <filename>\n\tquit program: quit");
			String tmp_statement;
			Console tmp_console;
			String[] tokens;
			while (true){
				tmp_console = System.console();
				tmp_statement = tmp_console.readLine();
				tokens = tmp_statement.split(" ");
				if (tmp_statement.equals("quit")) {
					break;
				} else if (tokens[0].equals("run")) {
					runScript(tokens[1], mem, disk, schema_manager);
				} else if (tmp_statement == "") {
					continue;
				}
				StatementInterpreter.executeStmt(tmp_statement, mem, disk, schema_manager);
			}
		} else {
			runScript(args[0], mem, disk, schema_manager);
		}
	}

	private static void runScript(String args, MainMemory mem, Disk disk, SchemaManager schema_manager) {		
		
		try {
			FileWriter performance_file_writer = new FileWriter("out_performance.txt");		
			try {
				String fileName = args;
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
		System.out.println("finished!");
	}
}