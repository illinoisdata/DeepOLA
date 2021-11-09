import copy

class Query:
	def __init__(self, operations):
		self.operations = operations

	def evaluate(self, df):
		current_input = df
		for operation in self.operations:
			current_input = operation.evaluate(current_input)
		return current_input

class QueryOLA(Query):
	def __init__(self, operations):
		self.operations = operations
		self.cache = {}

	def evaluate(self, relations, cache = True):
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
		current_input = relations
		self.cache = {}
		for operation in self.operations:
			current_input = operation.evaluate(current_input)
			if(type(current_input) == dict and len(current_input.keys()) == 1):
				current_input = current_input[list(current_input.keys())[0]]
			if not operation.is_mergeable and cache:
				self.cache[operation] = copy.deepcopy(current_input)
		return current_input

	def online_evaluate(self, old_result, df_updates):
		"""
		Returns the result of query after combining the old result and evaluation output on the current dataframe.

		@param old_result: The query output on current dataframe.
		@param df_update: The insert update to the dataframe after which we want to calculate the result.

		@return result: The combined result of the query on (original_data + df_update).
		"""

		# This will change to iterate till mergeable True.
		# After that operation, we would evaluate the remaining operations on the merged data.
		operated_till = -1
		current_input = df_updates
		for index,operation in enumerate(self.operations):
			operated_till += 1
			current_input = operation.evaluate(current_input)
			# ipdb.set_trace()
			if(type(current_input) == dict and len(current_input.keys()) == 1):
				current_input = current_input[list(current_input.keys())[0]]
			if not operation.is_mergeable:
				prev_result = self.cache[operation]
				current_input = operation.merge(prev_result, current_input) ## Update cache.
				self.cache[operation] = copy.deepcopy(current_input)
				print("Merged at Operation",operation)
				break

		merge_evaluated = False
		for index in range(operated_till+1, len(self.operations)):
			merge_evaluated = True
			operation = self.operations[index]
			current_input = operation.evaluate(current_input)

		if not merge_evaluated:
			current_input = self.operations[-1].merge(old_result, current_result)
		return current_input