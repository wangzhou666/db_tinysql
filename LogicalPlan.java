import storageManager.*;
import java.util.ArrayList;

public class LogicalPlan {

	static public void createTable(String relation_name, ArrayList<String> field_names, ArrayList<FieldType> field_types, 
		SchemaManager schema_manager) {
		
		Schema schema = new Schema(field_names, field_types);
		schema_manager.createRelation(relation_name, schema);

	}

	static public void dropTable(String relation_name, SchemaManager schema_manager) {

		schema_manager.deleteRelation(relation_name);

	}

	static public void insertTuple(String relation_name, ArrayList<String> field_names, ArrayList<String> field_values, 
		SchemaManager schema_manager) {

		Relation relation = schema_manager.getRelation(relation_name);
		Tuple tuple;

		int attr_amount = field_names.size();

		for (int i = 0; i < attr_amount; i++) {
			tuple = relation.createTuple();
			
		}

	}
}