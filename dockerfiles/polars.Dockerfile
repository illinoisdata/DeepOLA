### Build container
FROM python:3.8-bookworm

### Install Python Dependency
RUN apt-get update
RUN apt-get install vmtouch time
RUN pip install --upgrade pip

### Copy Polars Directory
COPY ./baselines/polars /polars

### Install Dependencies
WORKDIR /polars/tpch
RUN pip install -r requirements.txt

CMD ["/bin/bash"]
