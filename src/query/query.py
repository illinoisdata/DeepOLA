class Query:
	def __init__(self, operations):
		self.operations = operations

	def evaluate(self, df):
		current_input = df
		for operation in self.operations:
			current_input = operation.evaluate(current_input)
		return current_input

class QueryOLA:
	def __init__(self, operations):
		self.operations = operations

	def evaluate(self, df):
		current_input = df
		for operation in self.operations:
			current_input = operation.evaluate(current_input)
		return current_input

	def online_evaluate(self, old_result, df):
		current_result = df
		for operation in self.operations:
			current_result = operation.evaluate(current_result)
		return self.operations[-1].merge(old_result, current_result, evaluate = False)