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
    url='https://github.com/USEPA/ElectricityLCI',
    license='CC0',
    author='Matt Jamieson, Wesley Ingwersen, Greg Schively, TJ Ghosh, Ben Young, Troy Hottle',
    author_email='ingwersen.wesley@epa.gov',
    description='Create life cycle inventory data for regionalized electricity generation, mix of generation, mix of consumption, and distribution to the end-user in the United States.',
    install_requires=[
        'fedelemflowlist @ git+https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List#egg=fedelemflowlist',
        'StEWI @ git+https://github.com/USEPA/standardizedinventories#egg=StEWI',
        'scipy>=1.10',
        ],
    long_description=open('README.md').read(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: IDE",
        "Intended Audience :: Science/Research",
        "License :: CC0",
        "Programming Language :: Python :: 3.x",
        "Topic :: Utilities",
    ]
)
