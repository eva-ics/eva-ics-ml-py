__version__ = '0.0.1'

import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='evaics.ml',
    version=__version__,
    author='Bohemia Automation / Altertech',
    author_email='div@altertech.com',
    description='EVA ICS v4 Machine Learning Kit',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/eva-ics/eva-mlkit-client-python',
    packages=setuptools.find_packages(),
    license='Apache License 2.0',
    install_requires=[
        'requests>=2.26.0', 'pandas>=1.5.1', 'evaics>=0.2.5', 'tqdm>=4.45.0'
    ],
    classifiers=('Programming Language :: Python :: 3',
                 'License :: OSI Approved :: MIT License',
                 'Topic :: Software Development :: Libraries',
                 'Topic :: Scientific/Engineering :: Artificial Intelligence'),
)
