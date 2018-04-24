FROM python:3.6

ARG NEW_USER_UID
ARG NEW_USER

RUN apt-get -y update && \
    apt-get install -y vim openssh-client python-tox && \
    pip3 install twine sphinx sphinx_rtd_theme wheel && \
    pip3 install --pre github3.py

RUN useradd -u ${NEW_USER_UID} -U -d /home/${NEW_USER} -s /bin/bash -m ${NEW_USER}

USER ${NEW_USER}
WORKDIR /home/${NEW_USER}

# Also add a venv, in case it becomes useful later on. Strictly speaking venv shouldn't be needed inside a container.
RUN python3 -m venv venv
