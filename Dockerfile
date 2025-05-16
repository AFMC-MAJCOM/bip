FROM python:3.12.0-slim

COPY . /home/bip

WORKDIR /home/bip

RUN python3 -m pip install .

ENTRYPOINT ["bip"]

CMD ["--version"]
