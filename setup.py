from setuptools import setup, find_packages
from dss_dispatcher.__version__ import version

setup(
    name='ssbgp-dss-dispatcher',
    version=version,
    description='Dispatcher component for SS-BGP distributed simulation system',
    url='https://github.com/ssbgp/dss-dispatcher',
    license='MIT',
    author='David Fialho',
    author_email='fialho.david@protonmail.com',

    packages=find_packages(),

    package_data={
        'dss_dispatcher': [
            'tables.sql',
            'logs.ini'
        ],
    },

    entry_points={
        'console_scripts': [
            'dss-dispatcher=dss_dispatcher.main:main',
        ],
    }
)
