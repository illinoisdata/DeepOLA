FROM python:3.8-bookworm
RUN apt-get update
RUN apt-get install -y build-essential bison

## Download and Install Flex
WORKDIR /wanderjoin/flex
RUN wget https://launchpad.net/ubuntu/+archive/primary/+sourcefiles/flex/2.5.31-31/flex_2.5.31.orig.tar.gz
RUN tar -xvf flex_2.5.31.orig.tar.gz
WORKDIR /wanderjoin/flex/flex-2.5.31
ENV PATH=$PATH:/usr/local/m4/bin/
RUN ./configure --prefix=/usr/local/flex --build=aarch64-unknown-linux-gnu
RUN make
RUN make install
ENV PATH=$PATH:/usr/local/flex/bin
RUN flex --version

## Download and Install XDB
WORKDIR /wanderjoin
RUN git clone https://github.com/InitialDLab/XDB.git
WORKDIR /wanderjoin/XDB
RUN git checkout 2aa61db15cedf712bc27ecbf19b60e33be41ad2a
RUN ./configure
RUN make clean
RUN make
RUN make install

## Setup Postgres
RUN useradd -p wanderjoin postgres
RUN mkdir /usr/local/pgsql/data
RUN chown postgres /usr/local/pgsql/data
ENV PATH=$PATH:/usr/local/pgsql/bin/

COPY ./baselines/wanderjoin/queries /wanderjoin/queries/
COPY ./baselines/wanderjoin/setup-queries /wanderjoin/setup-queries/
COPY ./baselines/wanderjoin/experiment-setup.sh /wanderjoin/experiment-setup.sh
COPY ./baselines/wanderjoin/experiment-time.sh /wanderjoin/experiment-time.sh
COPY ./baselines/wanderjoin/experiment.sh /wanderjoin/experiment.sh

RUN chown -R postgres /wanderjoin
USER postgres
RUN initdb -D /usr/local/pgsql/data
WORKDIR /wanderjoin

CMD ["/bin/bash"]