from operations.base import BaseOperation
import pandas as pd

class WHERE(BaseOperation):
	def __init__(self, args):
		## Define validator for the args input.
		self.left = args['left']
		self.right = args['right']
		self.op = args['op']

	def evaluate(self, df):
		if self.op == '<':
			return df[df[self.left] < self.right]
		elif self.op == '>':
			return df[df[self.left] > self.right]
		elif self.op == '=':
			return df[df[self.left] == self.right]
		else:
			raise NotImplementedError

	def merge(self, old_result, current_result):
		return pd.concat([old_result,current_result])