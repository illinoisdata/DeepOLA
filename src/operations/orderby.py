from operations.base import BaseOperation
import pandas as pd

class ORDERBY(BaseOperation):
	def __init__(self, args):
		## Define validator for the args input.
		self.columns = args['columns']
		self.order = args['order']
		self.order_bool = [True if x == 'asc' else False for x in self.order]

	def evaluate(self, df):
		return df.sort_values(self.columns, ascending = self.order_bool)

	def merge(self, old_result, current_result):
		## Need to compute order by again after merging.
		return self.evaluate(pd.concat([old_result,current_result]))