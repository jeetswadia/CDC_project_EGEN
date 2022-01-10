FROM python:3.8

WORKDIR /

ADD ["CDC_Covid.py","requirement.txt","cdccovidjeet-34146c45c9ab.json","./"]

RUN pip install -r requirement.txt

CMD [ "python","./CDC_Covid.py" ]

