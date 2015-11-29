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
}