from setuptools import setup

setup(
    name='deepola',
    version='0.1',
    description='DeepOLA Python Module',
    author='CreateLab, UICU',
    author_email='sheoran2@illinois.edu',
    packages=['deepola','deepola.operations','deepola.query'],  # same as name
    install_requires=['numpy', 'pandas', 'polars', 'matplotlib', 'argparse'], # external packages as dependencies
)
