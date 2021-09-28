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

	def evaluate(self, df, cache = True):
		"""
		Returns the result of query on the input data frame.
		Note: When evaluating the result, keep track of the operation where
		mergeable is not true. We need to save the result at that point
		and merge at that layer.

		@param df: The dataframe to evaluate the query on.
		@param cache: To cache the intermediate results at operations where the result is not mergeable.

		@return result: The result of evaluating query on df.
		@return cache: The intermediate result of evaluating query on df.
		"""
		current_input = df
		for operation in self.operations:
			current_input = operation.evaluate(current_input)
		return current_input

	def online_evaluate(self, old_result, df_update):
		"""
		Returns the result of query after combining the old result and evaluation output on the current dataframe.

		@param old_result: The query output on current dataframe.
		@param df_update: The insert update to the dataframe after which we want to calculate the result.

		@return result: The combined result of the query on (original_data + df_update).
		"""

		# This will change to iterate till mergeable True.
		# After that operation, we would evaluate the remaining operations on the merged data.
		current_result = df_update
		for operation in self.operations:
			current_result = operation.evaluate(current_result)
		return self.operations[-1].merge(old_result, current_result, evaluate = False)