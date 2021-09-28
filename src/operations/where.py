from operations.base import BaseOperation
import pandas as pd

class WHERE(BaseOperation):
	"""docstring for WHERE"""
	def __init__(self, args):
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

	def merge(self, df1, df2, evaluate = True):
		if evaluate:
			result1 = self.evaluate(df1)
			result2 = self.evaluate(df2)
		else:
			result1 = df1
			result2 = df2

		return pd.concat([result1,result2])