FROM python
ADD . /todo
WORKDIR /todo
ENV PYTHONUNBUFFERED=1 
RUN pip install docker
RUN pip install -r requirements.txt 
CMD ["python","slave.py"] 

