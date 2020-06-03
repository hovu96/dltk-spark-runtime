FROM gcr.io/spark-operator/spark-py:v2.4.5

# root folder for everything that is dltk specific
ENV DLTK_DIR /dltk
RUN mkdir -p $DLTK_DIR

# install python packages
COPY pyspark-requirements.txt ${DLTK_DIR}/requirements.txt
RUN pip3 install --no-cache-dir -r ${DLTK_DIR}/requirements.txt
COPY driver/requirements.txt ${DLTK_DIR}/driver-requirements.txt
RUN pip3 install --no-cache-dir -r ${DLTK_DIR}/driver-requirements.txt

# dltk driver server
ENV DRIVER_DIR $DLTK_DIR/driver
RUN mkdir -p $DRIVER_DIR
COPY driver/*.py ${DRIVER_DIR}
EXPOSE 8888
