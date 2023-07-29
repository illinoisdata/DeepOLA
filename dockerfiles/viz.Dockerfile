FROM python:3.8-bookworm
COPY scripts/viz-requirements.txt ./requirements.txt
RUN apt-get update
RUN pip install -r requirements.txt

COPY ./scripts ./scripts

CMD ["/bin/bash"]
