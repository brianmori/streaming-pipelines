FROM python:3.9-slim

# Add a build argument for changed files
ARG CHANGED_FILES

# Set an environment variable with the changed files
ENV CHANGED_FILES=$CHANGED_FILES

# Set the working directory in the container
WORKDIR /app

# Install AWS CLI and jq
RUN apt-get update && apt-get install -y python3 jq && pip install awscli

# Install Python dependencies
COPY _app/requirements.txt .
RUN pip install -r requirements.txt

# Copy necessary files
COPY ./submission/ ./submission/
COPY _app/ ./

RUN chmod +x src/entrypoint.sh

ENTRYPOINT [ "src/entrypoint.sh" ]