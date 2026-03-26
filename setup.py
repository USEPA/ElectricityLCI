from setuptools import setup

setup(
    name='electricitylci',
    version='3.0.0',
    packages=['electricitylci'],
    package_data={
        'electricitylci': ["data/*.*",
                           "data/EFs/*.*",
                           "data/coal/2020/*.*",
                           "data/coal/2023/*.*",
                           "data/petroleum_inventory/*.*",
                           "data/renewables/2016/*.*",
                           "data/renewables/2020/*.*",
                           "modelconfig/*.yml",
                           "output/.gitignore",
                           ]
    },
    url='https://github.com/NETL-RIC/ElectricityLCI',
    license='CC0',
    author='Tyler W. Davis, Francis Hanna, Matthew Jamieson, Wesley W. Ingwersen, Greg Schivley, Ben Young, Tapajyoti Ghosh, Jing Li, Shirley Sam, Daniel Lee Young, Michael Srocka, and Troy A. Hottle',
    author_email='Mathew.Jamieson@netl.doe.gov',
    description='A Python package to create regional life cycle inventory models of U.S. electricity generation, consumption, and distribution using standardized facility and generation data for use with open-source LCA software.',
    install_requires=[
        'fedelemflowlist @ git+https://github.com/FLCAC-Admin/fedelemflowlist',
        'StEWI @ git+https://github.com/USEPA/standardizedinventories#egg=StEWI',
        'scipy>=1.10',
        'pandas<3.0',   # see issue 321
        'pytz',
        ],
    long_description=open('README.md').read(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "License :: CC0",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.x",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering",
        "Topic :: Utilities",
    ]
)
