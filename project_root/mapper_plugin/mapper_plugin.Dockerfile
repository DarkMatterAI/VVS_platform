FROM nvcr.io/nvidia/pytorch:24.07-py3 as base 

WORKDIR /opt/app

FROM base as install-from-pypi
RUN pip install -U nvidia-pytriton

FROM install-from-pypi AS image

ENV PYTHONUNBUFFERED=1

WORKDIR /opt/app

# COPY ./requirements.txt /opt/app/requirements.txt

# RUN pip install --no-cache-dir --upgrade -r /opt/app/requirements.txt

COPY ./src/server.py /opt/app 

# ENTRYPOINT []

# CMD python ./server.py

CMD ["python", "./server.py"]


# ARG FROM_IMAGE_NAME=nvcr.io/nvidia/pytorch:24.07-py3
# ARG BUILD_FROM

# FROM ${FROM_IMAGE_NAME} as base
# WORKDIR /opt/app

# # Use when build PyTriton from source
# FROM base as install-from-dist
# COPY dist/*.whl /opt/app
# RUN pip install /opt/app/*.whl

# # Install from pypi
# FROM base as install-from-pypi
# RUN pip install -U nvidia-pytriton

# FROM install-from-${BUILD_FROM} AS image

# ENV PYTHONUNBUFFERED=1

# WORKDIR /opt/app

# COPY examples/huggingface_resnet_pytorch/install.sh /opt/app
# RUN /opt/app/install.sh

# COPY examples/huggingface_resnet_pytorch/client.py /opt/app
# COPY examples/huggingface_resnet_pytorch/server.py /opt/app

# ENTRYPOINT []