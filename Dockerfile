FROM scimma/python-service-base:latest
ADD requirements.txt /root/requirements.txt
RUN python3.9 -m pip install -r /root/requirements.txt

ADD metrics_getOffsets.py /root/metrics_getOffsets.py

ENTRYPOINT ["python3.9", "/root/metrics_getOffsets.py"]