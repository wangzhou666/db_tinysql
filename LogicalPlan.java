import storageManager.*;
import java.util.ArrayList;

public class LogicalPlan {

	public static void createTable(String relation_name, ArrayList<String> field_names, ArrayList<FieldType> field_types, 
		SchemaManager schema_manager) {
		
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

	public static void insertTuple(String relation_name, ArrayList<String> field_names, ArrayList<String> field_values, 
		SchemaManager schema_manager, MainMemory mem) {

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
		int num_block_reading;
		String output_str = relation.getSchema().fieldNamesToString()+"\n";
		ArrayList<Tuple> tuples_in_block;

		while (num_blocks_read < num_blocks) {
			num_block_reading = Math.min(num_blocks - num_blocks_read, mem.getMemorySize());
			relation.getBlocks(num_blocks_read, 0, num_block_reading);
			for (int i = 0; i < num_block_reading; i++) {
				tuples_in_block = mem.getBlock(i).getTuples();
				for (Tuple t : tuples_in_block) {
					output_str += t.toString();
					output_str += "\n";
				}
			}
			num_blocks_read += num_block_reading;
		}
		System.out.println(output_str);

	}


}