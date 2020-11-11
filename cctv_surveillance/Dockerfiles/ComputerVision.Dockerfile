# based on python 3.7
# source: https://hub.docker.com/r/jjanzic/docker-python3-opencv
FROM jjanzic/docker-python3-opencv   

# Compile Dlib using cmake
RUN mkdir -p ~/dlib \
    && cd ~/dlib \
    && wget http://dlib.net/files/dlib-19.16.tar.bz2 \
    && tar xf dlib-19.16.tar.bz2 \
    && cd ~/dlib/dlib-19.16 \
    && mkdir build \
    && cd build \
    && cmake .. \
    && cmake --build . --config Release \
    && make -j${CPUCORES} \
    && cd ~/dlib/dlib-19.16/build \
    && make install \
    && ldconfig

# Install packages for Python
RUN pkg-config --libs --cflags dlib-1 \
    && cd ~/dlib/dlib-19.16 \
    && python setup.py install \
    && rm -rf ~/dlib /var/cache/apk/* /usr/share/man /usr/local/share/man /tmp/*


# Install Protocol buffers
RUN PROTOC_ZIP=protoc-3.7.1-linux-x86_64.zip \
    && curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v3.7.1/$PROTOC_ZIP  \
    && unzip -o $PROTOC_ZIP -d /usr/local bin/protoc  \
    && unzip -o $PROTOC_ZIP -d /usr/local 'include/*' \
    && rm -f $PROTOC_ZIP

ADD ./requirements_handcrafted.txt ./
RUN pip install -r requirements_handcrafted.txt

WORKDIR /usr/app

# Create output directories for the programs
RUN mkdir /usr/app/temp \
    && mkdir /usr/app/out

# Checkout the source code & compile protobuf
RUN git clone https://github.com/manojphatak/Video-Analytics.git Video-Analytics   \
    && cd Video-Analytics   \
    && git checkout dynamic \
    && cd cctv_surveillance

