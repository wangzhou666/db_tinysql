import storageManager.*;
import java.util.ArrayList;

public class StatementInterpreter {

	public static void executeStmt(String line, MainMemory mem, Disk disk, SchemaManager schema_manager) {
		String[] tokens = line.split(" ");
		int scan_pt = 0;

		if (tokens[0].equals("CREATE") && tokens[1].equals("TABLE")) { // to create a table
			scan_pt += 2;
			String relation_name = tokens[2];
			scan_pt += 1;
			
			ArrayList<String> field_names = new ArrayList<String>();
			ArrayList<FieldType> field_types = new ArrayList<FieldType>();
			String tmp_name;
			String tmp_field_type;
			boolean flag_break = false;

			for (int i = scan_pt; i < tokens.length; i += 2) {
				tmp_name = tokens[i];
				tmp_field_type = tokens[i+1];

				if (tmp_field_type.contains(")")) {
					flag_break = true;
				}
				tmp_name = tmp_name.replace("(","");
				tmp_name = tmp_name.replace(",","");
				tmp_field_type = tmp_field_type.replace(",","");
				tmp_field_type = tmp_field_type.replace(")","");

				field_names.add(tmp_name);
				if (tmp_field_type.equals("INT")) {
					field_types.add(FieldType.INT);
				} else if (tmp_field_type.equals("STR20")) {
					field_types.add(FieldType.STR20);
				} else {
					System.out.println("unknown type");
				}
				scan_pt += 2;

				if (flag_break) {
					break;
				}
			}

			Schema schema = new Schema(field_names, field_types);
			schema_manager.createRelation(relation_name, schema);

		} else if (tokens[0].equals("DROP") && tokens[1].equals("TABLE")) { // to delete a table
			scan_pt += 2;
			String relation_name = tokens[2];
			scan_pt += 1;

			schema_manager.deleteRelation(relation_name);
			
		} else if (tokens[0].equals("INSERT")) { // to insert value to a table
			
		} else if (tokens[0].equals("DELETE")) { // to delete value in a table
			
		} else if (tokens[0].equals("SELECT")) { // to display tuples
			
		} else {
			System.out.println("unknown statement");
		}
	}

}