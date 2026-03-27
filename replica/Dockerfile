FROM python:3.9-slim

WORKDIR /app

RUN pip install aiohttp

COPY raft_node.py .

CMD ["python", "raft_node.py"]
