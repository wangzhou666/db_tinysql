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
			int tokens_amount = findNextTokenContains(tokens, scan_pt, ")");
			int stop_pt = scan_pt + tokens_amount;

			for (int i = scan_pt; i < stop_pt; i += 2) {
				tmp_name = tokens[i];
				tmp_field_type = tokens[i+1];

				tmp_name = tmp_name.replace("(", "");
				tmp_name = tmp_name.replace(",", "");
				tmp_field_type = tmp_field_type.replace(",", "");
				tmp_field_type = tmp_field_type.replace(")", "");

				field_names.add(tmp_name);
				if (tmp_field_type.equals("INT")) {
					field_types.add(FieldType.INT);
				} else if (tmp_field_type.equals("STR20")) {
					field_types.add(FieldType.STR20);
				} else {
					System.out.println("unknown type");
				}
				scan_pt += 2;
			}

			LogicalPlan.createTable(relation_name, field_names, field_types, schema_manager);

		} else if (tokens[0].equals("DROP") && tokens[1].equals("TABLE")) { // to delete a table
			scan_pt += 2;
			String relation_name = tokens[2];
			scan_pt += 1;

			// LogicalPlan.dropTable(relation_name, schema_manager);

		} else if (tokens[0].equals("INSERT") && tokens[1].equals("INTO")) { // to insert value(s) to a table
			scan_pt += 2;
			String relation_name = tokens[2];
			scan_pt += 1;

			ArrayList<String> field_names = new ArrayList<String>();
			String tmp_name;
			int tokens_amount = findNextTokenContains(tokens, scan_pt, ")");
			int stop_pt = scan_pt + tokens_amount;

			for (int i = scan_pt; i < stop_pt; i++) {
				tmp_name = tokens[i];
				tmp_name = tmp_name.replace("(", "");
				tmp_name = tmp_name.replace(",", "");
				tmp_name = tmp_name.replace(")", "");

				field_names.add(tmp_name);
				scan_pt++;
			}

			if (tokens[scan_pt].equals("VALUES")) {
				scan_pt++;
				ArrayList<String> field_values = new ArrayList<String>();
				String tmp_value;
				tokens_amount = findNextTokenContains(tokens, scan_pt, ")");
				stop_pt = scan_pt + tokens_amount;

				for (int i = scan_pt; i < stop_pt; i++) {
					tmp_value = tokens[i];
					tmp_value = tmp_value.replace("(", "");
					tmp_value = tmp_value.replace("(", "");
					tmp_value = tmp_value.replace("(", "");

					field_values.add(tmp_value);
					scan_pt++;
				}

				LogicalPlan.createTuple(relation_name, field_names, field_values, schema_manager);

			} else if (tokens.[scan_pt].equals("SELECT")) {
				System.out.println("undefined statement");
			} else {
				System.out.println("unknown statement");
			}

		} else if (tokens[0].equals("DELETE")) { // to delete value in a table
			
		} else if (tokens[0].equals("SELECT")) { // to display tuples
			
		} else {
			System.out.println("unknown statement");
		}
	}

	// help find how many tokens to read
	static private int findNextTokenContains(String[] tokens, int sc_pt, String substr) {
		int i = sc_pt;
		while (i < tokens.length) {
			if (tokens[i].contains(substr)) {
				break;
			}
			i++;
		}
		return i-sc_pt+1;
	}
}