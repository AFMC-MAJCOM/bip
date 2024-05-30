FROM python:3.12.0-slim

ADD . /home/bip

WORKDIR /home/bip

RUN python3 -m pip install .

ENTRYPOINT ["bip"]

CMD ["--version"]
