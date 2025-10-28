from setuptools import setup

setup(
    name='electricitylci',
    version='2.0.0',
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
    author='Tyler W. Davis, Matthew Jamieson, Wesley W. Ingwersen, Greg Schivley, Ben Young, Tapajyoti Ghosh, Jing Li, Shirley Sam, Daniel Lee Young, Michael Srocka, and Troy A. Hottle',
    author_email='matthew.jamieson@netl.doe.gov',
    description='Create life cycle inventory data for regionalized electricity generation, mix of generation, mix of consumption, and distribution to the end-user in the United States.',
    install_requires=[
        'fedelemflowlist @ git+https://github.com/FLCAC-admin/Federal-LCA-Commons-Elementary-Flow-List#egg=fedelemflowlist',
        'StEWI @ git+https://github.com/USEPA/standardizedinventories#egg=StEWI',
        'scipy>=1.10',
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
