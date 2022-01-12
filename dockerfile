FROM python:3.7

WORKDIR /

ADD ["CDC_Covid.py","requirement.txt","cdcjeet-521e045bbf00.json","./"]

RUN pip install -r requirement.txt

CMD [ "python","./CDC_Covid.py" ]

