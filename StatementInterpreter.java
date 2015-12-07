import storageManager.*;
import java.util.ArrayList;

public class StatementInterpreter {

	public static void executeStmt(String line, MainMemory mem, Disk disk, SchemaManager schema_manager) {
		String[] tokens = line.split(" ");
		int scan_pt = 0;

		if (tokens.length == 0) {
			return;
		}

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
					assert false;
				}
				scan_pt += 2;
			}

			LogicalPlan.createTable(relation_name, field_names, field_types, schema_manager);

		} else if (tokens[0].equals("DROP") && tokens[1].equals("TABLE")) { // to delete a table
			scan_pt += 2;
			String relation_name = tokens[2];
			scan_pt += 1;

		 	LogicalPlan.dropTable(relation_name, schema_manager);

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
					tmp_value = tmp_value.replace(")", "");
					tmp_value = tmp_value.replace("\"", "");
					tmp_value = tmp_value.replace(",", "");

					field_values.add(tmp_value);
					scan_pt++;
				}

				LogicalPlan.insertTuple(relation_name, field_names, field_values, schema_manager, mem);

			} else if (tokens[scan_pt].equals("SELECT")) {
			
				assert tokens[scan_pt+1].equals("*");
				String from_relation_name = tokens[scan_pt+3];
				LogicalPlan.insertTuples(relation_name, from_relation_name, schema_manager, mem);


			} else {
				System.out.println("unknown statement");
				assert false;
			}

		} else if (tokens[0].equals("DELETE") && tokens[1].equals("FROM")) { // to delete value in a table
			
			String relation_name = tokens[2];
			String attribute_name = null;
			String attribute_value = null;
			if (tokens.length > 3) {
				if (tokens[3].equals("WHERE")) {
				attribute_name = tokens[4];
				attribute_value = tokens[6].replace("\"","");
				}
			} 
			LogicalPlan.deleteTuples(relation_name, attribute_name, attribute_value, schema_manager, mem);

		} else if (tokens[0].equals("SELECT")) { // to display tuples
			
			String[] reservedWords = new String[] {"FROM", "ORDER", "WHERE"};
			int amount_select_tokens = findClauseLength(tokens, scan_pt, reservedWords);
			scan_pt += amount_select_tokens;
			int amount_from_tokens = findClauseLength(tokens, scan_pt, reservedWords);

			boolean need_distinct;
			if (tokens[1].equals("DISTINCT")) {
				need_distinct = true;
			} else {
				need_distinct = false;
			}

			boolean need_projection;
			ArrayList<String> attr_proj_names = new ArrayList<String>();
			if (tokens[scan_pt-1].equals("*")) {
				need_projection = false;
			} else {
				need_projection = true;
				int amount_attr_proj;
				if (need_distinct) {
					amount_attr_proj = amount_select_tokens - 2;
				} else {
					amount_attr_proj = amount_select_tokens - 1;
				}
				for (int i =0; i < amount_attr_proj; i++) {
					attr_proj_names.add(tokens[scan_pt-i-1].replace(",", ""));
				}
			}

			ArrayList<String> from_table_names = new ArrayList<String>();
			for (int i = 1; i < amount_from_tokens; i++) {
				from_table_names.add(tokens[scan_pt+i].replace(",", ""));
			}
			scan_pt += amount_from_tokens;

			boolean has_condition = false;
			boolean need_order = false;
			String order_attr_name = null;
			String[] where_tokens = new String[] {};
			if (scan_pt == tokens.length) { // only select and from
				// end reading tokens
			} else if (tokens[scan_pt].equals("ORDER")) {
				order_attr_name = tokens[scan_pt+2];
				need_order = true;
			} else if (tokens[scan_pt].equals("WHERE")) {
				has_condition = true;
				int amount_where_tokens = findClauseLength(tokens, scan_pt, reservedWords);
				where_tokens = new String[amount_where_tokens-1];
				System.arraycopy(tokens, scan_pt+1, where_tokens, 0, amount_where_tokens-1);
				// do something
				scan_pt += amount_where_tokens;
				if (scan_pt == tokens.length) {
				 	// end reading
				} else if (tokens[scan_pt].equals("ORDER")) {
					need_order = true;
					order_attr_name = tokens[scan_pt+2];
				}
			}

			String where_infix = String.join(" ", where_tokens);
			where_infix = where_infix.replace("AND", "&");
			where_infix = where_infix.replace("OR", "|");
			where_infix = where_infix.replace("NOT", "!");
			String where_postfix = ShuntingYard.infixToPostfix(where_infix);
			String[] tokens_postfix = where_postfix.split(" ");

			if (from_table_names.size() == 1) {
				// no need to join
				if (has_condition) {
					if (!need_order && !need_distinct) {
						if (need_projection) {
							LogicalPlan.projectConditionedTable(from_table_names.get(0), schema_manager, mem, tokens_postfix, attr_proj_names);
						} else {
							LogicalPlan.displayConditionedTable(from_table_names.get(0), schema_manager, mem, tokens_postfix);
						}
					} else {
						declareInvalidStatement();
					}
				} else {
					if (need_order) {
						if (!need_projection && !need_distinct) {
							LogicalPlan.displayOrderTable(from_table_names.get(0), schema_manager, mem, order_attr_name);	
						} else {
							declareInvalidStatement();
						}					
					} else {
						if (need_distinct) {
							if (need_projection) {
								LogicalPlan.projectDistinctTable(from_table_names.get(0), schema_manager, mem, attr_proj_names);
							} else {
								LogicalPlan.displayDistinctTable(from_table_names.get(0), schema_manager, mem);
							}
						} else {
							if (need_projection) {
								LogicalPlan.projectTable(from_table_names.get(0), schema_manager, mem, attr_proj_names);
							} else {
								LogicalPlan.displayTable(from_table_names.get(0), schema_manager, mem);
							}
						}
					}
				}
			} else {
				// needs join
				if (has_condition) {
					if (need_distinct) {
						if (need_projection) {
							if (need_order) {
								
							} else {
								LogicalPlan.projectConditionedDistinctJoinTables(from_table_names, schema_manager, mem, tokens_postfix, attr_proj_names);
							}
						} else {
							declareInvalidStatement();
						}
					} else {
						if (need_projection) {
							LogicalPlan.projectConditionedJoinTables(from_table_names, schema_manager, mem, tokens_postfix, attr_proj_names);
						} else {
							if (need_order) {
								
							} else {
								LogicalPlan.displayConditionedJoinTables(from_table_names, schema_manager, mem, tokens_postfix);
							}
						}
					}
				} else {
					if (need_distinct) {
						
					} else {
						if (need_projection) {
							
						} else {
							LogicalPlan.displayJoinTables(from_table_names, schema_manager, mem);
						}
					}
				}
			}  
		} else {
			declareInvalidStatement();
		}
	}

	// help find how many tokens to read
	private static int findNextTokenContains(String[] tokens, int sc_pt, String substr) {
		int i = sc_pt;
		while (i < tokens.length) {
			if (tokens[i].contains(substr)) {
				break;
			}
			i++;
		}
		return i-sc_pt+1;
	}

	private static int findClauseLength(String[] tokens, int sc_pt, String[] reserved_words) {
		int i = sc_pt;
		boolean is_reserved_word;
		while (i < tokens.length) {
			i++;
			if (i == tokens.length) {
				break;
			}
			is_reserved_word = false;
			for (String s : reserved_words) {
				if (s.equals(tokens[i])) {
					is_reserved_word = true;
				}
			}
			if (is_reserved_word) {
				break;
			}
		}
		return i-sc_pt;
	}

	private static void declareInvalidStatement() {
		System.out.println("This structure has not been implemented");
	}

}