from operations.base import BaseOperation
import pandas as pd

class JOIN(BaseOperation):
	def __init__(self, args):
		self.left = args['left']
		self.right = args['right']
		self.left_on = args['left_on']
		self.right_on = args['right_on']
		self.type = args['type']
		self.alias = args['table_alias']

	def evaluate(self, dfs):
		dfs[self.alias] = dfs[self.left].merge(dfs[self.right], self.type, left_on = self.left_on, right_on = self.right_on)
		del dfs[self.left]
		del dfs[self.right]
		return dfs

	def merge(self, old_result, current_result):
		if self.type == 'inner':
			return pd.concat([old_result, current_result])
		else:
			raise NotImplementedError