import os
import re
import sys

from setuptools import setup, find_packages


CURRENT_PYTHON = sys.version_info[:2]
REQUIRED_PYTHON = (3, 7)

# borrowed from urllib3
if CURRENT_PYTHON < REQUIRED_PYTHON:
    sys.stderr.write("""
==========================
Unsupported Python version
==========================
This version of Morph-KGC requires Python {}.{}, but you're trying to install it on Python {}.{}.
""".format(*(REQUIRED_PYTHON + CURRENT_PYTHON)))
    sys.exit(1)


# borrowed from SQLAlchemy
with open(os.path.join(os.path.dirname(__file__), 'src', 'morph_kgc', '_version.py')) as file:
    version = (re.compile(r""".*__version__ = ["'](.*?)['"]""", re.S).match(file.read()).group(1))


with open(os.path.join(os.path.dirname(__file__), 'README.md'), 'r', encoding='utf-8') as file:
    readme = file.read()
    # remove morph logo
    readme = re.sub("<p[^>]*>", "", readme)
    readme = re.sub("<img[^>]*>", "", readme)
    readme = re.sub("</?p[^>]*>", "", readme)


with open(os.path.join(os.path.dirname(__file__), 'requirements.txt')) as file:
    requirements = [line.strip().replace(' ', '') for line in file.readlines()]


setup(
    name='morph_kgc',
    version=version,
    author='Julián Arenas-Guerrero',
    author_email='arenas.guerrero.julian@outlook.com',
    license='Apache 2.0',
    description='Powerful [R2]RML engine to create RDF knowledge graphs from heterogeneous data sources.',
    keywords='Morph-KGC, RDF, R2RML, RML, RML-star, Knowledge Graphs, Data Integration',
    long_description=readme,
    long_description_content_type='text/markdown',
    url='https://github.com/oeg-upm/morph-kgc',
    project_urls={
        'Documentation': 'https://morph-kgc.readthedocs.io/en/latest/documentation/',
        'Source code': 'https://github.com/oeg-upm/morph-kgc',
        'Issue tracker': 'https://github.com/oeg-upm/morph-kgc/issues',
    },
    include_package_data=True,
    packages=find_packages('src'),
    package_dir={'': 'src'},
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'Topic :: Software Development :: Pre-processors',
        'Topic :: Database',
        'Topic :: Utilities',
        'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator'
    ],
    install_requires=requirements,
    python_requires='>={}.{}, <4'.format(*(REQUIRED_PYTHON)),
)
