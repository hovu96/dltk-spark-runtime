FROM python:3

VOLUME [ "/data" ]

# relay folder
ENV RELAY_DIR /relay
RUN mkdir -p $RELAY_DIR

# install python packages
COPY relay/requirements.txt ${RELAY_DIR}/requirements.txt
RUN pip3 install --no-cache-dir -r ${RELAY_DIR}/requirements.txt

# copy libs
COPY lib/ $RELAY_DIR/

# copy relay sources
COPY relay/*.py ${RELAY_DIR}

EXPOSE 8888 8889 8890
WORKDIR $RELAY_DIR
ENTRYPOINT python $RELAY_DIR/relay.py
STOPSIGNAL SIGINT