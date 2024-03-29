# neuprint-utilities [![Picture](https://raw.github.com/janelia-flyem/janelia-flyem.github.com/master/images/HHMI_Janelia_Color_Alternate_180x40.png)](http://www.janelia.org)

[![Build Status](https://travis-ci.org/connectome-neuprint/neuprint-utilities.svg?branch=master)](https://travis-ci.org/connectome-neuprint/neuprint-utilities)
[![GitHub last commit](https://img.shields.io/github/last-commit/connectome-neuprint/neuprint-utilities.svg)](https://github.com/connectome-neuprint/neuprint-utilities)
[![GitHub commit merge status](https://img.shields.io/github/commit-status/badges/shields/master/5d4ab86b1b5ddfb3c4a70a70bd19932c52603b8c.svg)](https://github.com/connectome-neuprint/neuprint-utilities)
[![Python 3.7](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-360/)
[![Requirements Status](https://requires.io/github/connectome-neuprint/neuprint-utilities/requirements.svg?branch=master)](https://requires.io/github/connectome-neuprint/neuprint-utilities/requirements/?branch=master)

## Summary
Utility programs for NeuPrint

### Create docker container for sync-neuprint
```
 docker build -t sync_neuprint:1.0 .
```

### Running sync-neuprint with docker

Create a file named .env that contains NEUPRINT_APPLICATION_CREDENTIALS environment variable

```
docker run -it --env-file .env sync_neuprint \
python /app/scripts/sync_neuprint_mongo.py --v --manifold dev --neuprint pre --debug
```

For more information, see the [Wiki](https://github.com/connectome-neuprint/neuprint-utilities/wiki)
