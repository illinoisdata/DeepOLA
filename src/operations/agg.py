from operations.base import BaseOperation
import pandas as pd

class AGG(BaseOperation):
	def __init__(self, args):
		"""
		key: A list of columns that acts as the key that can be used to join the results.
		If the agg is coming after group by then this key can be the group by key.
		column: The column on which the aggregate function is to be evaluated on.
		function: The aggregate function.
		"""
		self.column = args['column']
		self.op = args['op']
		self.alias = args['alias']
		self.key = args['key']

	def evaluate(self, df):
		## If it is * => Take the primary key of the relation.
		if self.op not in ['sum','avg','count']:
			raise NotImplementedError
		result = df.agg({self.column: self.op})
		result[self.alias] = result[self.column]
		return result

	def merge(self, old_result, current_result):
		if self.op == 'count' or self.op == 'sum':
			if(type(old_result) == pd.DataFrame and type(current_result) == pd.DataFrame):
				joined_result = pd.merge(old_result, current_result, how = 'inner', on = self.key, suffixes = ['_l','_r'])
				joined_result[self.alias] = joined_result[self.column + '_l'] + joined_result[self.column + '_r']
				return joined_result[ list(self.key) + list(self.column)]
			else:
				return (old_result + current_result)
		elif self.op == 'avg':
			raise NotImplementedError