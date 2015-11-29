import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import storageManager.*;

public class ScriptInterpreter {

	public static void main(String[] args) {
		
		MainMemory mem = new MainMemory();
		Disk disk = new Disk();
		SchemaManager schema_manager = new SchemaManager(mem, disk);
		
		disk.resetDiskIOs();
    	disk.resetDiskTimer();

		try {
			String fileName = args[0];
			File file = new File(fileName);
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				// System.out.println(line);
				StatementInterpreter.executeStmt(line, mem, disk, schema_manager);
			}
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(schema_manager.getRelation("course"));
		System.out.println(schema_manager.getRelation("course2"));
	}
}