# neuprint-utilities [![Picture](https://raw.github.com/janelia-flyem/janelia-flyem.github.com/master/images/HHMI_Janelia_Color_Alternate_180x40.png)](http://www.janelia.org)

[![Build Status](https://travis-ci.org/JaneliaSciComp/neuprint-utilities.svg?branch=master)](https://travis-ci.org/JaneliaSciComp/neuprint-utilities)
[![GitHub last commit](https://img.shields.io/github/last-commit/google/skia.svg)](https://github.com/JaneliaSciComp/neuprint-utilities)
[![GitHub commit merge status](https://img.shields.io/github/commit-status/badges/shields/master/5d4ab86b1b5ddfb3c4a70a70bd19932c52603b8c.svg)](https://github.com/JaneliaSciComp/neuprint-utilities)

## Summary
Utility programs for NeuPrint

## Stored data
Every hour, an entry is made into the nptest* index of ElasticSEarch with metrics data in three categories:

#### Neurons
These numbers are extracted from NeuPrint via a Cypher query for number of neurons. Here is an example query for the numbber of traced neurons:

```{"cypher": "MATCH (n:`hemibrain-Neuron`{status:'Traced'}) RETURN count(n)"}```

The following metrics are stored:
- NEURONS_0.5assign
- NEURONS_anchor
- NEURONS_leaves
- NEURONS_orphan
- NEURONS_orphan_hotknife
- NEURONS_prelim_roughly_traced
- NEURONS_roughly_traced
- NEURONS_traced
- NEURONS_unimportant

NEURONS_TOTAL_traced is also stored, and is the sum of NEURONS_prelim_roughly_traced, NEURONS_roughly_traced, and NEURONS_traced.

#### Incomplete

#### Totals
