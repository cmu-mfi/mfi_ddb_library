from setuptools import setup, find_packages

setup(
    name='ddb',
    version='0.1',
    packages=find_packages(include=['ddb.stream_to_mqtt_spb']),  # Ensure core is included
    install_requires=[         # Add dependencies from requirements.txt
        line.strip() for line in open('requirements.txt').readlines()
    ],
)
