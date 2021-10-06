import setuptools


with open("README.md", "r") as fh:
    long_description = fh.read()

with open("VERSION", "r") as fh:
    v = fh.read().replace("\n", "")

with open("requirements.txt") as r:
    requirements = list(filter(None, r.read().split("\n")[0:]))


setuptools.setup(
    name="Morph-KGC",
    version=v,
    author="Julián Arenas-Guerrero",
    author_email="arenas.guerrero.julian@outlook.com",
    license="Apache 2.0",
    description="Morph-KGC is an engine that constructs RDF knowledge graphs from heterogeneous data sources with "
                "R2RML and RML mapping languages. Morph-KGC is built on top of pandas and it leverages mapping "
                "partitions to significantly reduce execution times and memory consumption for large data sources.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/oeg-upm/Morph-KGC",
    include_package_data=True,
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "Topic :: Software Development :: Pre-processors",
        "Topic :: Database",
        "Topic :: Utilities",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator"
    ],
    install_requires=requirements,
    python_requires='>=3.7',
)
