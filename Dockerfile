FROM icr.io/codeengine/python:latest


WORKDIR cosunarch
ADD *.py ./
RUN ls -lR .
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN /usr/local/bin/python -m pip install -e .
