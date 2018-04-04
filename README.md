# OpenARC

A functional reactive graph database backed by PostgreSQL

In layman's terms, OpenARC let's you build graphs of objects that react to one another. Think of it as Excel, but for programmers.

# Getting Started

### Prerequisites

OpenARC requires Python 3+ and PostgreSQL 10+.

Consult your operating system documentation for details on how to install these requirements.

### Install OpenARC

Use `pip` to install the last release:

```sh
pip install openarc --user
```

Live on the edge by cloning our repository:

```sh
git clone https://www.github.com/kchoudhu/openarc
cd openarc
pip install . --user
```

### (Optional) Prepare database

If you don't already have a database to point to, you can use the makefile to spin up your own local instance to work with:

```sh
cd openarc
make dbmshardinit
```

### Configure OpenARC

Tell OpenARC where to find its configuration information:

```sh
export OPENARC_CFG_DIR=/config/lives/here
```

If ```OPENARC_CFG_DIR``` is not set, OpenARC will assume that its configuration is in the current directory.

OpenARC's configuration file is, unsurprisingly, called ```openarc.toml```. A sample file is available in the ```/cfg``` directory of the project distribution.

# Next Steps

### Run the tests

If you have the project distribution, take a look under ```openarc/tests``` for a wide variety of use cases. Assuming your environment is set up, you can execute the tests by executing

```sh
make test
```

### Take a look at the examples

Interesting examples of OpenARC usage live under ```openarc/examples```. Each example is thoroughly documented and comes with a README; the hope is that they will open your eyes as to what is possible with OpenARC.

### Read the documentation

Documentation is distributed with the code, and can be generated by executing

```sh
make docs
```
