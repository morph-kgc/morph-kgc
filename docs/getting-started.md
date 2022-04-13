<figure markdown>
  ![Morph-KGC](assets/logo.png){ width="300" }
</figure>

In the following we provide different ways to install and run Morph-KGC. [Here](https://github.com/oeg-upm/Morph-KGC/wiki/Configuration) you can find out how to generate the configuration file. If you are using relational databases you may need to install additional libraries, check [here](https://github.com/oeg-upm/Morph-KGC/wiki/Relational-Databases).

## PyPi

PyPi is the fastest way to install Morph-KGC:
```
pip install morph-kgc
```

To run the engine and generate the output RDF you just need to execute the following:
```
python3 -m morph_kgc config.ini
```

[Here](https://github.com/oeg-upm/Morph-KGC/wiki/Configuration) you can see how to generate the configuration file. It is also possible to run Morph-KGC as a library with [RDFlib](https://rdflib.readthedocs.io/en/stable/):

```python
import morph_kgc

# generate the triples and load them to an RDFlib graph
graph = morph_kgc.materialize('/path/to/config.ini')

# work with the graph
graph.query(' SELECT DISTINCT ?classes WHERE { ?s a ?classes } ')
graph.serialize(destination='result.ttl', format='turtle')
```

## From Source

You can also grab the latest source code from this GitHub repository. Clone this repository:
```
git clone https://github.com/oeg-upm/Morph-KGC.git
```

Access the root directory of the repository:
```
cd Morph-KGC
```

Install Morph-KGC:
```
pip3 install .
```

Finally, you can run the engine:
```
python3 -m morph_kgc config.ini
```
