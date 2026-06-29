from setuptools import setup, find_packages

setup(
    name='mfi_ddb',
    version='1.0.0',
    description='MFI DDB: Python library for streaming data',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    packages=find_packages(),  # Ensure core is included
    install_requires=[         # Add dependencies from requirements.txt
        line.strip() for line in open('requirements.txt').readlines()
    ],
    author='Carnegie Mellon University, Manufacturing Futures Institute',
    url='https://github.com/cmu-mfi/mfi_ddb_library',
    python_requires='>=3.8,<3.14',   
    license='BSD-3-Clause'    
)
