from setuptools import setup, find_packages

setup(
    name="kalshi-research",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'inflect',
        'pandas',
        'numpy',
        'scipy',
        'matplotlib',
        'seaborn'
    ],
) 