# onbuild copies . to /usr/src/app/
From python:2.7.9-onbuild
Maintainer J. Fernando Sánchez @balkian

# RUN pip --cert cacert.pem install -r -v  requirements.txt
ADD . /usr/src/app/

RUN pip install -e "/usr/src/app/[server]"
ENTRYPOINT ["bitter"]
