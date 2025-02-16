# FROM nvcr.io/nvidia/pytorch:24.07-py3 as base 

# WORKDIR /opt/app

# FROM base as install-from-pypi
# RUN pip install -U nvidia-pytriton

# FROM install-from-pypi AS image

# ENV PYTHONUNBUFFERED=1

# WORKDIR /opt/app

# # COPY ./requirements.txt /opt/app/requirements.txt

# # RUN pip install --no-cache-dir --upgrade -r /opt/app/requirements.txt

# COPY ./src/mapper_model.py /opt/app 

# COPY ./src/roberta_zinc_480m_1_2_bb_mapper.pt /opt/app

# COPY ./src/server.py /opt/app 

# CMD ["python", "./server.py"]

