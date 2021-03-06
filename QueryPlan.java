import storageManager.*;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Stack;

public class QueryPlan {

	public static void createTable(String relation_name, ArrayList<String> field_names, 
		ArrayList<FieldType> field_types, SchemaManager schema_manager) {
		
		Schema schema = new Schema(field_names, field_types);
		schema_manager.createRelation(relation_name, schema);

	}

	public static void dropTable(String relation_name, SchemaManager schema_manager) {

		schema_manager.deleteRelation(relation_name);

	}

	public static void deleteTuples(String relation_name, String attribute_name, String attribute_value, 
		SchemaManager schema_manager, MainMemory mem) {

		Relation relation = schema_manager.getRelation(relation_name);
		if (attribute_name == null) {	
			relation.deleteBlocks(0);
		} else {
			Block mem_blk;
			int num_relation_blocks = relation.getNumOfBlocks();
			ArrayList<Tuple> tuples_in_block;
			Field field_to_check;
			boolean flag_delete;
			for (int i = 0; i < num_relation_blocks; i++) {
				relation.getBlock(i, 0);
				mem_blk = mem.getBlock(0);
				tuples_in_block = mem_blk.getTuples();
				mem_blk.clear();
				flag_delete = false;
				for (Tuple t : tuples_in_block) {
					field_to_check = t.getField(attribute_name);
					if (!field_to_check.str.equals(attribute_value)) {
						mem_blk.appendTuple(t);
					} else {
						flag_delete = true;
					}
				}
				if (flag_delete) {
					relation.setBlock(i, 0);
				}
			}
		}

	}

	public static void insertTuple(String relation_name, ArrayList<String> field_names, 
		ArrayList<String> field_values, SchemaManager schema_manager, MainMemory mem) {

		Relation relation = schema_manager.getRelation(relation_name);
		Tuple tuple = relation.createTuple();

		int attr_amount = field_names.size();
		String tmp_field_name;
		String tmp_field_value;

		for (int i = 0; i < attr_amount; i++) {
			tmp_field_name = field_names.get(i);
			tmp_field_value = field_values.get(i);

			if (tuple.getField(tmp_field_name).type == FieldType.INT) {
				if (tmp_field_value.equals("NULL")) {
					tmp_field_value = "0";
				}
				tuple.setField(tmp_field_name, Integer.parseInt(tmp_field_value));
			} else if (tuple.getField(tmp_field_name).type == FieldType.STR20) {
				tuple.setField(tmp_field_name, tmp_field_value);
			}
		}
		// append a tuple to relation
		int num_relation_blocks = relation.getNumOfBlocks();
		Block mem_blk;
		if (num_relation_blocks == 0) {
			mem_blk = mem.getBlock(0);
			mem_blk.clear();
			mem_blk.appendTuple(tuple);
			relation.setBlock(0, 0);
		} else {
			relation.getBlock(num_relation_blocks - 1, 0);
			mem_blk = mem.getBlock(0);
			if (mem_blk.isFull()) {
				mem_blk.clear();
				mem_blk.appendTuple(tuple);
				relation.setBlock(num_relation_blocks, 0);
			} else {
				mem_blk.appendTuple(tuple);
				relation.setBlock(num_relation_blocks - 1, 0);
			}	
		}

	}

	public static void insertTuples(String relation_name, String from_relation_name, 
		SchemaManager schema_manager, MainMemory mem) {

		Relation to_relation = schema_manager.getRelation(relation_name);
		Relation from_relation = schema_manager.getRelation(from_relation_name);
		
		int num_blocks = from_relation.getNumOfBlocks();
		int num_blocks_copied = 0;
		int num_blocks_coping;
		
		while (num_blocks_copied < num_blocks) {
			num_blocks_coping = Math.min(num_blocks - num_blocks_copied, mem.getMemorySize());
			from_relation.getBlocks(num_blocks_copied, 0, num_blocks_coping);
			to_relation.setBlocks(to_relation.getNumOfBlocks(), 0, num_blocks_coping);
			num_blocks_copied += num_blocks_coping;
		}

	}

	public static void displayTable(String relation_name, SchemaManager schema_manager, MainMemory mem) {

		Relation relation = schema_manager.getRelation(relation_name);

		int num_blocks = relation.getNumOfBlocks();
		int num_blocks_read = 0;
		int num_blocks_reading;
		String output_str = relation.getSchema().fieldNamesToString()+"\n\n";
		ArrayList<Tuple> tuples_in_block;

		while (num_blocks_read < num_blocks) {
			num_blocks_reading = Math.min(num_blocks - num_blocks_read, mem.getMemorySize());
			relation.getBlocks(num_blocks_read, 0, num_blocks_reading);
			for (int i = 0; i < num_blocks_reading; i++) {
				tuples_in_block = mem.getBlock(i).getTuples();
				for (Tuple t : tuples_in_block) {
					output_str += t.toString();
					output_str += "\n";
				}
			}
			num_blocks_read += num_blocks_reading;
		}
		System.out.println("\n\n");
		System.out.println(output_str);

	}

	public static void projectTable(String relation_name, 
		SchemaManager schema_manager, MainMemory mem, ArrayList<String> attribute_names) {

		Relation relation = schema_manager.getRelation(relation_name);
		String prefixAllowed = relation_name + ".";
		String output_str = "";

		for (String attr_name : attribute_names) {
			output_str += attr_name + "\t";
		}
		output_str += "\n\n";

		int num_blocks = relation.getNumOfBlocks();
		int num_blocks_read = 0;
		int num_blocks_reading;
		ArrayList<Tuple> tuples_in_block;
		Field tmp_field;

		while (num_blocks_read < num_blocks) {
			num_blocks_reading = Math.min(num_blocks - num_blocks_read, mem.getMemorySize());
			relation.getBlocks(num_blocks_read, 0, num_blocks_reading);
			for (int i = 0; i < num_blocks_reading; i++) {
				tuples_in_block = mem.getBlock(i).getTuples();
				for (Tuple t : tuples_in_block) {
					for (String attr : attribute_names) {
						if (attr.contains(prefixAllowed)) {
							attr = attr.replace(prefixAllowed, "");
						}
						tmp_field = t.getField(attr);
						output_str += tmp_field.toString() + "\t";
					}
					output_str += "\n";
				}
			}
			num_blocks_read += num_blocks_reading;
		}
		System.out.println("\n\n");
		System.out.println(output_str);

	}

	public static void projectConditionedTable(String relation_name, 
		SchemaManager schema_manager, MainMemory mem, String[] postfix_tokens, ArrayList<String> attribute_names) {

		Relation relation = schema_manager.getRelation(relation_name);
		String prefixAllowed = relation_name + ".";
		String output_str = "";

		for (String attr_name : attribute_names) {
			output_str += attr_name + "\t";
		}
		output_str += "\n\n";

		int num_blocks = relation.getNumOfBlocks();
		int num_blocks_read = 0;
		int num_blocks_reading;
		ArrayList<Tuple> tuples_in_block;
		Field tmp_field;

		while (num_blocks_read < num_blocks) {
			num_blocks_reading = Math.min(num_blocks - num_blocks_read, mem.getMemorySize());
			relation.getBlocks(num_blocks_read, 0, num_blocks_reading);
			for (int i = 0; i < num_blocks_reading; i++) {
				tuples_in_block = mem.getBlock(i).getTuples();
				for (Tuple t : tuples_in_block) {
					if (!checkTupleCondition(t, postfix_tokens)) {
						continue;
					}
					for (String attr : attribute_names) {
						if (attr.contains(prefixAllowed)) {
							attr = attr.replace(prefixAllowed, "");
						}
						tmp_field = t.getField(attr);
						output_str += tmp_field.toString() + "\t";
					}
					output_str += "\n";
				}
			}
			num_blocks_read += num_blocks_reading;
		}
		System.out.println("\n\n");
		System.out.println(output_str);

	}

	// helper method to determine whether a collection of tuples contains a tuple
	private static boolean hasThisTuple(Tuple tp, ArrayList<Tuple> tps) {
		boolean compare_result = false;
		Field tmp_field_1;
		Field tmp_field_2;
		for (Tuple t : tps) {
			for (int i = 0; i < tp.getNumOfFields(); i++) {
				tmp_field_1 = tp.getField(i);
				tmp_field_2 = t.getField(i);
				if (tmp_field_1.type == FieldType.STR20) {
					if (!tmp_field_1.str.equals(tmp_field_2.str)) {
						break;
					}
				} else {
					if (tmp_field_1.integer != tmp_field_2.integer) {
						break;
					}
				}
				compare_result = true;
			}
			if (compare_result) {
				break;
			}
		}
		return compare_result;
	}

	private static boolean hasThisCombination(Tuple tp, ArrayList<Tuple> tps, ArrayList<String> attribute_names) {
		boolean compare_result = false;
		Field tmp_field_1;
		Field tmp_field_2;
		for (Tuple t : tps) {
			for (String attr : attribute_names) {
				tmp_field_1 = tp.getField(attr);
				tmp_field_2 = t.getField(attr);
				if (tmp_field_1.type == FieldType.STR20) {
					if (!tmp_field_1.str.equals(tmp_field_2.str)) {
						break;
					}
				} else {
					if (tmp_field_1.integer != tmp_field_2.integer) {
						break;
					}
				}
				compare_result = true;
			}
			if (compare_result) {
				break;
			}
		}
		return compare_result;
	}

	public static void displayDistinctTable(String relation_name, 
		SchemaManager schema_manager, MainMemory mem) {

		Relation relation = schema_manager.getRelation(relation_name);

		int num_blocks = relation.getNumOfBlocks();
		int num_blocks_read = 0;
		int num_blocks_reading;
		String output_str = relation.getSchema().fieldNamesToString()+"\n\n";
		ArrayList<Tuple> tuples_in_block;
		ArrayList<Tuple> tuples_to_display = new ArrayList<Tuple>();

		while (num_blocks_read < num_blocks) {
			num_blocks_reading = Math.min(num_blocks - num_blocks_read, mem.getMemorySize());
			relation.getBlocks(num_blocks_read, 0, num_blocks_reading);
			for (int i = 0; i < num_blocks_reading; i++) {
				tuples_in_block = mem.getBlock(i).getTuples();
				for (Tuple t : tuples_in_block) {
					if (!hasThisTuple(t, tuples_to_display)) {
						tuples_to_display.add(t);
					}
				}
			}
			num_blocks_read += num_blocks_reading;
		}
		for (Tuple t : tuples_to_display) {
			output_str += t.toString() + "\n";
		}
		System.out.println("\n\n");
		System.out.println(output_str);

	}

	public static void projectDistinctTable(String relation_name, 
		SchemaManager schema_manager, MainMemory mem, ArrayList<String> attribute_names) {

		Relation relation = schema_manager.getRelation(relation_name);
		String prefixAllowed = relation_name + ".";
		String output_str = "";

		int num_blocks = relation.getNumOfBlocks();
		int num_blocks_read = 0;
		int num_blocks_reading;
		ArrayList<String> attribute_names_1 = new ArrayList<String>();

		for (String attr_name : attribute_names) {
			output_str += attr_name + "\t";
			if (attr_name.contains(prefixAllowed)) {
				attribute_names_1.add(attr_name.replace(prefixAllowed, ""));
			} else {
				attribute_names_1.add(attr_name);
			}
		}
		output_str += "\n\n";

		ArrayList<Tuple> tuples_in_block;
		ArrayList<Tuple> tuples_to_display = new ArrayList<Tuple>();

		while (num_blocks_read < num_blocks) {
			num_blocks_reading = Math.min(num_blocks - num_blocks_read, mem.getMemorySize());
			relation.getBlocks(num_blocks_read, 0, num_blocks_reading);
			for (int i = 0; i < num_blocks_reading; i++) {
				tuples_in_block = mem.getBlock(i).getTuples();
				for (Tuple t : tuples_in_block) {
					if (!hasThisCombination(t, tuples_to_display, attribute_names_1)) {
						tuples_to_display.add(t);
					}
				}
			}
			num_blocks_read += num_blocks_reading;
		}

		Field tmp_field;

		for (Tuple t : tuples_to_display) {
			for (String attr : attribute_names_1) {
				tmp_field = t.getField(attr);
				output_str += tmp_field.toString() + "\t";
			}
			output_str += "\n";
		}
		System.out.println("\n\n");
		System.out.println(output_str);

	}

	public static void projectConditionedDistinctTable(String relation_name, 
		SchemaManager schema_manager, MainMemory mem, String[] postfix_tokens, ArrayList<String> attribute_names) {

		Relation relation = schema_manager.getRelation(relation_name);
		String prefixAllowed = relation_name + ".";
		String output_str = "";

		int num_blocks = relation.getNumOfBlocks();
		int num_blocks_read = 0;
		int num_blocks_reading;
		ArrayList<String> attribute_names_1 = new ArrayList<String>();

		for (String attr_name : attribute_names) {
			output_str += attr_name + "\t";
			if (attr_name.contains(prefixAllowed)) {
				attribute_names_1.add(attr_name.replace(prefixAllowed, ""));
			} else {
				attribute_names_1.add(attr_name);
			}
		}
		output_str += "\n\n";

		ArrayList<Tuple> tuples_in_block;
		ArrayList<Tuple> tuples_to_display = new ArrayList<Tuple>();

		while (num_blocks_read < num_blocks) {
			num_blocks_reading = Math.min(num_blocks - num_blocks_read, mem.getMemorySize());
			relation.getBlocks(num_blocks_read, 0, num_blocks_reading);
			for (int i = 0; i < num_blocks_reading; i++) {
				tuples_in_block = mem.getBlock(i).getTuples();
				for (Tuple t : tuples_in_block) {
					if (!hasThisCombination(t, tuples_to_display, attribute_names_1) && checkTupleCondition(t, postfix_tokens)) {
						tuples_to_display.add(t);
					}
				}
			}
			num_blocks_read += num_blocks_reading;
		}

		Field tmp_field;

		for (Tuple t : tuples_to_display) {
			for (String attr : attribute_names_1) {
				tmp_field = t.getField(attr);
				output_str += tmp_field.toString() + "\t";
			}
			output_str += "\n";
		}
		System.out.println("\n\n");
		System.out.println(output_str);

	}

	public static void displayOrderTable(String relation_name, 
		SchemaManager schema_manager, MainMemory mem, String order_attribute) {

		if (order_attribute == null) {
			return;
		}

		Relation relation = schema_manager.getRelation(relation_name);

		int num_blocks = relation.getNumOfBlocks();
		int num_blocks_read = 0;
		int num_blocks_reading;
		String output_str = relation.getSchema().fieldNamesToString()+"\n\n";
		ArrayList<Tuple> tuples_in_block;
		TreeMap<Comparable, ArrayList<Tuple>> attribute_to_tuple = new TreeMap<Comparable, ArrayList<Tuple>>();

		while (num_blocks_read < num_blocks) {
			num_blocks_reading = Math.min(num_blocks - num_blocks_read, mem.getMemorySize());
			relation.getBlocks(num_blocks_read, 0, num_blocks_reading);
			for (int i = 0; i < num_blocks_reading; i++) {
				tuples_in_block = mem.getBlock(i).getTuples();
				for (Tuple t : tuples_in_block) {
					if (t.getField(order_attribute).type == FieldType.STR20) {
						if (!attribute_to_tuple.containsKey(t.getField(order_attribute).str)) {
							attribute_to_tuple.put(t.getField(order_attribute).str, new ArrayList<Tuple>());
						}
						attribute_to_tuple.get(t.getField(order_attribute).str).add(t);		
					} else {
						if (!attribute_to_tuple.containsKey(new Integer(t.getField(order_attribute).integer))) {
							attribute_to_tuple.put(new Integer(t.getField(order_attribute).integer), new ArrayList<Tuple>());
						}
						attribute_to_tuple.get(new Integer(t.getField(order_attribute).integer)).add(t);	
					}
				}
			}
			num_blocks_read += num_blocks_reading;
		}

		ArrayList<Tuple> tuples_in_value;
		for (Comparable c : attribute_to_tuple.keySet()) {
			tuples_in_value = attribute_to_tuple.get(c);
			for (Tuple t : tuples_in_value) {
				output_str += t.toString() + "\n";
			}
		}
		System.out.println("\n\n");
		System.out.println(output_str);

	}

	public static void displayConditionedOrderTable(String relation_name, 
		SchemaManager schema_manager, MainMemory mem, String[] postfix_tokens, String order_attribute) {

		if (order_attribute == null) {
			return;
		}

		Relation relation = schema_manager.getRelation(relation_name);

		int num_blocks = relation.getNumOfBlocks();
		int num_blocks_read = 0;
		int num_blocks_reading;
		String output_str = relation.getSchema().fieldNamesToString()+"\n\n";
		ArrayList<Tuple> tuples_in_block;
		TreeMap<Comparable, ArrayList<Tuple>> attribute_to_tuple = new TreeMap<Comparable, ArrayList<Tuple>>();

		while (num_blocks_read < num_blocks) {
			num_blocks_reading = Math.min(num_blocks - num_blocks_read, mem.getMemorySize());
			relation.getBlocks(num_blocks_read, 0, num_blocks_reading);
			for (int i = 0; i < num_blocks_reading; i++) {
				tuples_in_block = mem.getBlock(i).getTuples();
				for (Tuple t : tuples_in_block) {
					if (!checkTupleCondition(t, postfix_tokens)) {
						continue;
					}
					if (t.getField(order_attribute).type == FieldType.STR20) {
						if (!attribute_to_tuple.containsKey(t.getField(order_attribute).str)) {
							attribute_to_tuple.put(t.getField(order_attribute).str, new ArrayList<Tuple>());
						}
						attribute_to_tuple.get(t.getField(order_attribute).str).add(t);		
					} else {
						if (!attribute_to_tuple.containsKey(new Integer(t.getField(order_attribute).integer))) {
							attribute_to_tuple.put(new Integer(t.getField(order_attribute).integer), new ArrayList<Tuple>());
						}
						attribute_to_tuple.get(new Integer(t.getField(order_attribute).integer)).add(t);	
					}
				}
			}
			num_blocks_read += num_blocks_reading;
		}

		ArrayList<Tuple> tuples_in_value;
		for (Comparable c : attribute_to_tuple.keySet()) {
			tuples_in_value = attribute_to_tuple.get(c);
			for (Tuple t : tuples_in_value) {
				output_str += t.toString() + "\n";
			}
		}
		System.out.println("\n\n");
		System.out.println(output_str);

	}

	public static void projectConditionedDistinctOrderTable(String relation_name, 
		SchemaManager schema_manager, MainMemory mem, String[] postfix_tokens, 
		ArrayList<String> attribute_names, String order_attribute) {

		if (order_attribute == null) {
			return;
		}

		Relation relation = schema_manager.getRelation(relation_name);

		int num_blocks = relation.getNumOfBlocks();
		int num_blocks_read = 0;
		int num_blocks_reading;
		String output_str = "";
		ArrayList<Tuple> tuples_in_block;
		TreeMap<Comparable, ArrayList<Tuple>> attribute_to_tuple = new TreeMap<Comparable, ArrayList<Tuple>>();

		while (num_blocks_read < num_blocks) {
			num_blocks_reading = Math.min(num_blocks - num_blocks_read, mem.getMemorySize());
			relation.getBlocks(num_blocks_read, 0, num_blocks_reading);
			for (int i = 0; i < num_blocks_reading; i++) {
				tuples_in_block = mem.getBlock(i).getTuples();
				for (Tuple t : tuples_in_block) {
					if (!checkTupleCondition(t, postfix_tokens)) {
						continue;
					}
					if (t.getField(order_attribute).type == FieldType.STR20) {
						if (!attribute_to_tuple.containsKey(t.getField(order_attribute).str)) {
							attribute_to_tuple.put(t.getField(order_attribute).str, new ArrayList<Tuple>());
						}
						attribute_to_tuple.get(t.getField(order_attribute).str).add(t);		
					} else {
						if (!attribute_to_tuple.containsKey(new Integer(t.getField(order_attribute).integer))) {
							attribute_to_tuple.put(new Integer(t.getField(order_attribute).integer), new ArrayList<Tuple>());
						}
						attribute_to_tuple.get(new Integer(t.getField(order_attribute).integer)).add(t);	
					}
				}
			}
			num_blocks_read += num_blocks_reading;
		}

		ArrayList<Tuple> tuples_in_order = new ArrayList<Tuple>();
		ArrayList<Tuple> tuples_in_value;
		for (Comparable c : attribute_to_tuple.keySet()) {
			tuples_in_value = attribute_to_tuple.get(c);
			for (Tuple t : tuples_in_value) {
				tuples_in_order.add(t);
			}
		}

		ArrayList<Tuple> tuples_to_display = new ArrayList<Tuple>();
		ArrayList<String> attribute_names_1 = new ArrayList<String>();
		for (String attr_name : attribute_names) {
			output_str += attr_name + "\t";
			attribute_names_1.add(attr_name);
		}
		output_str += "\n\n";

		for (Tuple t : tuples_in_order) {
			if (!hasThisCombination(t, tuples_to_display, attribute_names_1)) {
				tuples_to_display.add(t);
			}
		}

		Field tmp_field;
		for (Tuple t : tuples_to_display) {
			for (String attr : attribute_names_1) {
				tmp_field = t.getField(attr);
				output_str += tmp_field.toString() + "\t";
			}
			output_str += "\n";
		}

		System.out.println(output_str);

	}

	private static String joinTables(ArrayList<String> table_names, 
		SchemaManager schema_manager, MainMemory mem) {

		// create joined table
		String joined_name = "";
		ArrayList<String> joined_field_names = new ArrayList<String>();
		ArrayList<FieldType> joined_field_types = new ArrayList<FieldType>();
		Relation tmp_relation;
		Schema tmp_schema;
		for (String name : table_names) {
			joined_name += name;
			tmp_relation = schema_manager.getRelation(name);
			tmp_schema = tmp_relation.getSchema();
			for (String s : tmp_schema.getFieldNames()) {
				joined_field_names.add(name+"."+s);
			}
			joined_field_types.addAll(tmp_schema.getFieldTypes());
		}
		Schema joined_schema = new Schema(joined_field_names, joined_field_types);
		schema_manager.createRelation(joined_name, joined_schema);
		
		// create cross product tuples
		//ArrayList<ArrayList<Tuple>> joining_tables = new ArrayList<ArrayList<Tuple>>();
		ArrayList<ArrayList<ArrayList<String>>> joining_tables_value = new ArrayList<ArrayList<ArrayList<String>>>();
		//ArrayList<Tuple> tmp_tuples;
		ArrayList<ArrayList<String>> tmp_table_values;
		ArrayList<String> tmp_tuple_value;
		int num_blocks;
		int num_blocks_read;
		int num_blocks_reading;
		ArrayList<Tuple> tuples_in_block;

		for (String name : table_names) {
			tmp_relation = schema_manager.getRelation(name);
			//tmp_tuples = new ArrayList<Tuple>();
			tmp_table_values = new ArrayList<ArrayList<String>>();

			num_blocks = tmp_relation.getNumOfBlocks();
			num_blocks_read = 0;
			
			while (num_blocks_read < num_blocks) {
				num_blocks_reading = Math.min(num_blocks - num_blocks_read, mem.getMemorySize());
				tmp_relation.getBlocks(num_blocks_read, 0, num_blocks_reading);
				for (int i = 0; i < num_blocks_reading; i++) {
					tuples_in_block = mem.getBlock(i).getTuples();
					for (Tuple t : tuples_in_block) {
						//tmp_tuples.add(t);
						tmp_tuple_value = new ArrayList<String>();
						for (int j = 0; j < t.getNumOfFields(); j++) {
							tmp_tuple_value.add(t.getField(j).toString());
						}
						tmp_table_values.add(tmp_tuple_value);
					}
				}
				num_blocks_read += num_blocks_reading;
			}
			//joining_tables.add(tmp_tuples);
			joining_tables_value.add(tmp_table_values);
		}

		ArrayList<ArrayList<String>> joined_tuples_value = getJoinedValues(joining_tables_value);
		for (ArrayList<String> tpv : joined_tuples_value) {
			insertTuple(joined_name, joined_field_names, tpv, schema_manager, mem);
		}

		return joined_name;
	}

	private static ArrayList<ArrayList<String>> getJoinedValues(ArrayList<ArrayList<ArrayList<String>>> joining_tables_value) {
		assert joining_tables_value.size() > 0;
		ArrayList<ArrayList<String>> results_tuples_value;
		if (joining_tables_value.size() == 1) {
			return joining_tables_value.get(0);
		} else if (joining_tables_value.size() == 2) {
			results_tuples_value = new ArrayList<ArrayList<String>>();
			ArrayList<String> tmp_tuple_value;
			ArrayList<ArrayList<String>> table_values_0 = joining_tables_value.get(0);
			ArrayList<ArrayList<String>> table_values_1 = joining_tables_value.get(1);
			for (ArrayList<String> tpv_0 : table_values_0) {
				for (ArrayList<String> tpv_1 : table_values_1) {
					tmp_tuple_value = new ArrayList<String>();
					tmp_tuple_value.addAll(tpv_0);
					tmp_tuple_value.addAll(tpv_1);
					results_tuples_value.add(tmp_tuple_value);
				}
			}
			return results_tuples_value;
		} else {
			ArrayList<ArrayList<ArrayList<String>>> first_two_joining_tables = new ArrayList<ArrayList<ArrayList<String>>>();
			first_two_joining_tables.add(joining_tables_value.get(0));
			first_two_joining_tables.add(joining_tables_value.get(1));
			ArrayList<ArrayList<ArrayList<String>>> first_two_joined_tables = new ArrayList<ArrayList<ArrayList<String>>>();
			first_two_joined_tables.add(getJoinedValues(first_two_joining_tables));
			for (int i = 2; i < joining_tables_value.size(); i++) {
				first_two_joined_tables.add(joining_tables_value.get(i));
			}
			return getJoinedValues(first_two_joined_tables);
		}
	}

	public static void displayJoinTables(ArrayList<String> table_names, 
		SchemaManager schema_manager, MainMemory mem) {
		String joined_name = joinTables(table_names, schema_manager, mem);
		displayTable(joined_name, schema_manager, mem);
		dropTable(joined_name, schema_manager);
	}

	private static boolean checkTupleCondition(Tuple checking_tuple, String[] postfix_tokens) {
	
		if (postfix_tokens.length == 0) {
			return true;
		}

		Schema schema = checking_tuple.getSchema();
		ArrayList<String> table_field_names = schema.getFieldNames();
		Stack<String> result_stack = new Stack<String>();
		String current_token;
		String tmp_operand;
		String tmp_operand_1;
		String tmp_operand_2;
		boolean tmp_op_val_1;
		boolean tmp_op_val_2;
		boolean result_val;
		int tmp_op_int_1 = 0;
		int tmp_op_int_2 = 0;
		String tmp_op_str_1 = "";
		String tmp_op_str_2 = "";
		boolean field_is_str20;
		int tmp_int_result;

		for (int i = 0; i < postfix_tokens.length; i++) {
			current_token = postfix_tokens[i];
			if (isOperator(current_token)) {
				if (current_token.equals("!")) {

					tmp_operand = result_stack.pop();
					assert tmp_operand.equals("true") || tmp_operand.equals("false");					
					if (tmp_operand.equals("true")) {
						result_stack.push("false");
					} else {
						result_stack.push("true");
					}  
				} else {
					tmp_operand_2 = result_stack.pop();
					tmp_operand_1 = result_stack.pop();
					assert !current_token.equals("^");

					if (current_token.equals("&") || current_token.equals("|")) {
						
						assert tmp_operand_2.equals("true") || tmp_operand_2.equals("false");
						assert tmp_operand_1.equals("true") || tmp_operand_1.equals("false");
						if (tmp_operand_1.equals("true")) {
							tmp_op_val_1 = true;
						} else {
							tmp_op_val_1 = false;
						}
						if (tmp_operand_2.equals("true")) {
							tmp_op_val_2 = true;
						} else {
							tmp_op_val_2 = false;
						}

						if (current_token.equals("&")) {
							result_val = tmp_op_val_1 && tmp_op_val_2;
						} else {
							result_val = tmp_op_val_1 || tmp_op_val_2;
						}

						if (result_val) {
							result_stack.push("true");
						} else {
							result_stack.push("false");
						}
					} else {
						// handle +-*/<>=
						if (isAttribute(tmp_operand_1, table_field_names)) {
							if (checking_tuple.getField(tmp_operand_1).type == FieldType.STR20) {
								field_is_str20 = true;
								tmp_op_str_1 = checking_tuple.getField(tmp_operand_1).toString();
							} else {
								field_is_str20 = false;
								tmp_op_int_1 = Integer.parseInt(checking_tuple.getField(tmp_operand_1).toString());
							}
						} else {
							if (tmp_operand_1.contains("\"")) {
								field_is_str20 = true;
								tmp_op_str_1 = tmp_operand_1.replace("\"", "");
							} else {
								field_is_str20 = false;
								tmp_op_int_1 = Integer.parseInt(tmp_operand_1);
							}
						}

						if (isAttribute(tmp_operand_2, table_field_names)) {
							if (field_is_str20) {
								tmp_op_str_2 = checking_tuple.getField(tmp_operand_2).toString();
							} else {
								tmp_op_int_2 = Integer.parseInt(checking_tuple.getField(tmp_operand_2).toString());
							}
						} else {
							if (field_is_str20) {
								tmp_op_str_2 = tmp_operand_2.replace("\"", "");
							} else {
								tmp_op_int_2 = Integer.parseInt(tmp_operand_2);
							}
						}

						if (field_is_str20) {
							assert current_token.equals("=");
							if (tmp_op_str_1.equals(tmp_op_str_2)) {
								result_stack.push("true");
							} else {
								result_stack.push("false");
							}
						} else {
							if (current_token.equals("=")) {
								if (tmp_op_int_1 == tmp_op_int_2) {
									result_stack.push("true");
								} else {
									result_stack.push("false");
								}
							} else if (current_token.equals(">")) {
								if (tmp_op_int_1 > tmp_op_int_2) {
									result_stack.push("true");
								} else {
									result_stack.push("false");
								}
							} else if (current_token.equals("<")) {
								if (tmp_op_int_1 < tmp_op_int_2) {
									result_stack.push("true");
								} else {
									result_stack.push("false");
								}
							} else if (current_token.equals("+")) {
								tmp_int_result = tmp_op_int_1 + tmp_op_int_2;
								result_stack.push(Integer.toString(tmp_int_result));
							} else if (current_token.equals("-")) {
								tmp_int_result = tmp_op_int_1 - tmp_op_int_2;
								result_stack.push(Integer.toString(tmp_int_result));
							} else if (current_token.equals("*")) {
								tmp_int_result = tmp_op_int_1 * tmp_op_int_2;
								result_stack.push(Integer.toString(tmp_int_result));
							} else if (current_token.equals("/")) {
								tmp_int_result = tmp_op_int_1 / tmp_op_int_2;
								result_stack.push(Integer.toString(tmp_int_result));
							} else {
								assert false;
							}
						}
					}
				}
			} else {
				result_stack.push(current_token);
			}
		}
		if (result_stack.peek().equals("true")) {
			return true;
		} else {
			return false;
		}
	}

	private static boolean isAttribute(String token, ArrayList<String> field_names) {
		return field_names.contains(token);
	}

	private static boolean isOperator(String token) {
		if ("|&<>!=-+/*^".contains(token)) {
			return true;
		} else {
			return false;
		}
	}

	public static void displayConditionedTable(String relation_name, 
		SchemaManager schema_manager, MainMemory mem, String[] postfix_tokens) {

		Relation relation = schema_manager.getRelation(relation_name);

		int num_blocks = relation.getNumOfBlocks();
		int num_blocks_read = 0;
		int num_blocks_reading;
		String output_str = relation.getSchema().fieldNamesToString()+"\n\n";
		ArrayList<Tuple> tuples_in_block;

		while (num_blocks_read < num_blocks) {
			num_blocks_reading = Math.min(num_blocks - num_blocks_read, mem.getMemorySize());
			relation.getBlocks(num_blocks_read, 0, num_blocks_reading);
			for (int i = 0; i < num_blocks_reading; i++) {
				tuples_in_block = mem.getBlock(i).getTuples();
				for (Tuple t : tuples_in_block) {
					if (checkTupleCondition(t, postfix_tokens)) {
						output_str += t.toString();
						output_str += "\n";
					}
				}
			}
			num_blocks_read += num_blocks_reading;
		}
		System.out.println("\n\n");
		System.out.println(output_str);

	}

	public static void displayConditionedJoinTables(ArrayList<String> table_names, 
		SchemaManager schema_manager, MainMemory mem, String[] postfix_tokens) {

		String joined_name = joinTables(table_names, schema_manager, mem);
		displayConditionedTable(joined_name, schema_manager, mem, postfix_tokens);
		dropTable(joined_name, schema_manager);

	}

	public static void projectConditionedJoinTables(ArrayList<String> table_names, 
		SchemaManager schema_manager, MainMemory mem, String[] postfix_tokens, ArrayList<String> attribute_names) {

		String joined_table_name = joinTables(table_names, schema_manager, mem);
		projectConditionedTable(joined_table_name, schema_manager, mem, postfix_tokens, attribute_names);
		dropTable(joined_table_name, schema_manager);

	}

	public static void projectConditionedDistinctJoinTables(ArrayList<String> table_names, 
		SchemaManager schema_manager, MainMemory mem, String[] postfix_tokens, ArrayList<String> attribute_names) {

		String joined_table_name = joinTables(table_names, schema_manager, mem);
		projectConditionedDistinctTable(joined_table_name, schema_manager, mem, postfix_tokens, attribute_names);
		dropTable(joined_table_name, schema_manager);

	}

	public static void displayConditionedOrderJoinTables(ArrayList<String> table_names, 
		SchemaManager schema_manager, MainMemory mem, String[] postfix_tokens, String order_attribute) {

		String joined_table_name = joinTables(table_names, schema_manager, mem);
		displayConditionedOrderTable(joined_table_name, schema_manager, mem, postfix_tokens, order_attribute);
		dropTable(joined_table_name, schema_manager);

	}

	public static void projectConditionedDistinctOrderJoinTables(ArrayList<String> table_names, 
		SchemaManager schema_manager, MainMemory mem, String[] postfix_tokens, ArrayList<String> attribute_names, String order_attribute) {

		String joined_table_name = joinTables(table_names, schema_manager, mem);
		projectConditionedDistinctOrderTable(joined_table_name, schema_manager, mem, postfix_tokens, attribute_names, order_attribute);
		dropTable(joined_table_name, schema_manager);

	}



}