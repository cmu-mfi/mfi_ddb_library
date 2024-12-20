from setuptools import setup, find_packages

setup(
    name='mfi_ddb',
    version='0.1',
    packages=find_packages(),  # Ensure core is included
    install_requires=[         # Add dependencies from requirements.txt
        line.strip() for line in open('requirements.txt').readlines()
    ],
    author='Shobhit Aggarwal',
    author_email='shobhit@cmu.edu',
    url='https://github.com/cmu-mfi/mfi_ddb_library'
)
