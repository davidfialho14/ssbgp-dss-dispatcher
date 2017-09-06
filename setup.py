from setuptools import setup, find_packages
from dss_dispatcher.__version__ import version

setup(
    name='ssbgp-dss-dispatcher',
    version=version,
    description='Dispatcher component for SS-BGP distributed simulation system',
    url='https://github.com/davidfialho14/ssbgp-dss-dispatcher/blob/master/README.md',
    license='MIT',
    author='David Fialho',
    author_email='fialho.david@protonmail.com',

    packages=find_packages(),

    install_requires=[],

    package_data={
        'ssbgp-dss-dispatcher': ['dss_dispatcher/tables.sql'],
    },

    extras_require={
        'test': ['pytest'],
    },

    entry_points={
        'console_scripts': [
            'ssbgp-dss-dispatcher=dss_dispatcher.main:main',
        ],
    }
)
