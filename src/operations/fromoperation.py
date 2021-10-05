from operations.base import BaseOperation
import pandas as pd

class FROM(BaseOperation):
	def __init__(self, args):
		## Define validator for the args input.
		self.tables = args['tables']
		## Load each of these tables?
		## Set tracker for these dataframes?

	def evaluate(self, table_set):
		return table_set