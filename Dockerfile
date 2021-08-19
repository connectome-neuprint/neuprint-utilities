FROM centos:8

RUN dnf install -y \
        wget

RUN mkdir /tmp-install && \
    cd /tmp-install && \
    wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh \
         -O miniconda-install.sh && \
    bash miniconda-install.sh -b -p /miniconda && \
    rm miniconda-install.sh && \
    rm -rf /tmp-install

WORKDIR /app

ENV PATH=/miniconda/bin:${PATH}
ENV CONFIG_SERVER_URL=http://config.int.janelia.org/
ENV NEUPRINT_APPLICATION_CREDENTIALS=

RUN conda config --set always_yes yes --set changeps1 no && \
    conda update -q conda && \
    conda install python=3.9

COPY bin/sync-conda-requirements.yml conda-requirements.yml

RUN conda env update -n base -f conda-requirements.yml && \
    rm -f conda-requirements.yml

COPY bin/sync_neuprint_mongo.py /app/scripts/sync_neuprint_mongo.py

