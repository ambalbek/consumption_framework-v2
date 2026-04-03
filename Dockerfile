FROM python:3.11-slim-bookworm

WORKDIR /app

ADD requirements.txt .
# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# ADD the necessary files
ADD main.py .
ADD consumption consumption
ADD entrypoint.sh .
# Make the entrypoint script executable
RUN chmod +x entrypoint.sh


ENTRYPOINT ["./entrypoint.sh"]
CMD ["--help"]
