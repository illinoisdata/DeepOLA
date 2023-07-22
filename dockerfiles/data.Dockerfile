FROM python:3.8-bookworm

WORKDIR /dataset
COPY scripts/data-gen.sh /dataset/scripts/data-gen.sh
COPY scripts/convert-to-parquet.py /dataset/scripts/convert-to-parquet.py

### Build tpch-kit
RUN git clone https://github.com/gregrahn/tpch-kit.git
WORKDIR /dataset/tpch-kit/dbgen
RUN make MACHINE=LINUX DATABASE=POSTGRESQL

### Conversion Scripts
COPY scripts/data-generation-requirements.txt /dataset/scripts/data-generation-requirements.txt
WORKDIR /dataset/scripts
RUN pip install --upgrade pip
RUN pip install -r data-generation-requirements.txt

CMD ["/bin/bash"]
