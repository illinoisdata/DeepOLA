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
		self.key = args['key']
		self.column = args['column']
		self.op = args['op']

	def evaluate(self, df):
		## If it is * => Take the primary key of the relation.
		if self.op == 'count':
			return df[self.column].count()
		elif self.op == 'avg':
			return df[self.column].mean()
		elif self.op == 'sum':
			return df[self.column].sum()
		else:
			raise NotImplementedError

	def merge(self, df1, df2, evaluate = True):
		if evaluate:
			result1 = self.evaluate(df1)
			result2 = self.evaluate(df2)
		else:
			result1 = df1
			result2 = df2
		if self.op == 'count' or self.op == 'sum':
			if(type(df1) == pd.DataFrame and type(df2) == pd.DataFrame):
				joined_result = pd.merge(df1, df2, how = 'inner', on = self.key, suffixes = ['_l','_r'])
				joined_result[self.column] = joined_result[self.column + '_l'] + joined_result[self.column + '_r']
				return joined_result[ list(self.key) + list(self.column)]
			else:
				return (df1 + df2)
		elif self.op == 'avg':
			raise NotImplementedError