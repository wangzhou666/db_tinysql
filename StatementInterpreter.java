import storageManager.*;

public class StatementInterpreter {

	public static void executeStmt(String line, MainMemory mem, Disk disk, SchemaManager schema_manager) {
		String[] tokens = line.split(" ");
		int scan_pt = 0;

		if (tokens[0].equals("CREATE") && tokens[1].equals("TABLE")) { // to create a table
			scan_pt += 2;

		} else if (tokens[0].equals("DROP") && tokens[1].equals("TABLE")) { // to delete a table
			
		} else if (tokens[0].equals("INSERT")) { // to insert value to a table
			
		} else if (tokens[0].equals("DELETE")) { // to 
			
		} else if (tokens[0].equals("SELECT")) {
			
		} else {
			System.out.println("unknown statement")
		}
	}

}