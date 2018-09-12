from distutils.core import setup

setup(
    name='ElectricityLCI',
    version='1.0alpha',
    packages=['electricitylci'],
    package_data={'electricitylci': ["data/*.*", "output/*.*"]},
    url='https://github.com/USEPA/ElectricityLCI',
    license='CC0',
    author='Wesley Ingwersen',
    author_email='ingwersen.wesley@epa.gov',
    description='Create life cycle inventory data for regionalized electricity generation, mix of generation, mix of consumption, and distribution to the end-user in the United States.',
    install_requires = ['numpy','pandas','olca-ipc','openpyxl'],
    long_description = open('README.md').read(),
    classifiers = [
        "Development Status :: Alpha",
        "Environment :: IDE",
        "Intended Audience :: Science/Research",
        "License :: CC0",
        "Programming Language :: Python :: 3.x",
        "Topic :: Utilities",
    ]
)
