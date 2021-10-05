from operations.base import BaseOperation
import pandas as pd

class GROUPBY(BaseOperation):
	def __init__(self, args):
		## Define validator for the args input.
		self.columns = args['columns']

	def evaluate(self, df):
		return df.groupby(self.columns)

	def merge(self, old_result, current_result):
		raise NotImplementedError