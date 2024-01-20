FROM jupyter/pyspark-notebook

WORKDIR /home/jovyan/work

COPY requirements.txt requirements.txt

RUN python -m pip install -r requirements.txt

ADD . /home/jovyan/work/

EXPOSE 8888

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--allow-root"]