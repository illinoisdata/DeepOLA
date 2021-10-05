from operations.base import BaseOperation
import pandas as pd

class SELECT(BaseOperation):
	def __init__(self, args):
		## Define validator for the args input.
		self.columns = args['columns']

	def evaluate(self, df):
		df.reset_index(inplace=True)
		return df[self.columns]

	def merge(self, old_result, current_result):
		return pd.concat([old_result,current_result])