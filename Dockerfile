
#FROM digi0ps/python-opencv-dlib:latest
FROM spmallick/opencv-docker:opencv

#ADD requirements_docker.txt /usr/src/app
ADD * /usr/src/app/

RUN cd /usr/src/app \
#    && git clone https://github.com/manojphatak-hcl/Video-Analytics.git \
#    && cd Video-Analytics \
#    && git checkout dockerize \
    && pip3 install -r requirements_docker.txt

#COPY Dockerfile  /usr/src/app/Video-Analytics

CMD tail -f /dev/null

