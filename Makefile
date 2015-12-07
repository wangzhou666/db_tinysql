all: StorageManager ScriptInterpreter

StorageManager: storageManager/*.java
	javac storageManager/*.java

ScriptInterpreter: ScriptInterpreter.java StatementInterpreter.java LogicalPlan.java ShuntingYard.java
	javac ScriptInterpreter.java StatementInterpreter.java LogicalPlan.java ShuntingYard.java

clean: 
	rm storageManager/*.class *.class