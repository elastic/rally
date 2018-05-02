FROM ubuntu:16.04

ARG NEW_USER_UID
ARG NEW_USER

RUN apt-get -y update && \
    apt-get install -y curl python3 python3-pip vim openssh-client \
    git make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev \
    openjdk-8-jdk

RUN pip3 install tox virtualenvwrapper twine sphinx sphinx_rtd_theme wheel

RUN curl --progress-bar -o openjdk-10.0.1_linux-x64_bin.tar.gz \
    https://download.java.net/java/GA/jdk10/10.0.1/fb4372174a714e6b8c52526dc134031e/10/openjdk-10.0.1_linux-x64_bin.tar.gz && \
    mkdir -p /opt/jdk-10 ; tar xzf openjdk-10.0.1_linux-x64_bin.tar.gz -C /opt/jdk-10 --strip-components 1 && rm openjdk-10.0.1_linux-x64_bin.tar.gz

RUN useradd -u ${NEW_USER_UID} -U -d /home/${NEW_USER} -s /bin/bash -m ${NEW_USER}

USER ${NEW_USER}
WORKDIR /home/${NEW_USER}

RUN git clone https://github.com/yyuu/pyenv.git ~/.pyenv

ENV HOME /home/${NEW_USER}
ENV PYENV_ROOT=/home/${NEW_USER}/.pyenv
ENV PATH=$PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH
ENV JAVA_HOME=/opt/jdk-10
ENV RUNTIME_JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

RUN pyenv install 3.4.8
RUN pyenv install 3.5.5
RUN pyenv install 3.6.5
RUN pyenv global 3.6.5 3.5.5 3.4.8
