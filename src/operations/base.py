import pandas as pd

class BaseOperation:
	def __init__(self):
		pass

	def evaluate(self, df):
		raise NotImplementedError

	@property
	def is_mergeable(self):
		raise NotImplementedError

	def merge(self, df1, df2):
		raise NotImplementedError

	@property
	def is_dynamic(self):
		raise NotImplementedError