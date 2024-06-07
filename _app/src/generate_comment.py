import os
import re
import logging
import requests
import boto3
from util import get_logger, get_api_key, check_aws_creds, get_git_creds, get_assignment, \
    get_submission_dir  # , get_changed_files
from openai import OpenAI

logger = get_logger()
client = OpenAI(api_key=get_api_key())

submission_dir = get_submission_dir()
git_token, repo, pr_number = get_git_creds()
s3_bucket = check_aws_creds()


def get_submissions(submission_dir: str) -> dict:
    submissions = {}
    try:
        jobs_dir = os.path.join(submission_dir)
        jobs_files = [
            f for f in os.listdir(jobs_dir)
            if os.path.isfile(os.path.join(jobs_dir, f))
        ]

        files_found = jobs_files
    except FileNotFoundError:
        logger.error(f"Directory not found: {submission_dir}")
        return None

    logger.info(f"Files: {files_found}")
    for sub_file in files_found:
        file_path = os.path.join(submission_dir, sub_file)
        if os.path.isfile(file_path) and (
                re.match(r'.*\.py', sub_file)
                or re.match(r'.*\.sql', sub_file)):
            try:
                with open(file_path, "r") as file:
                    file_content = file.read()
                if re.search(r'\S', file_content):
                    submissions[file_path] = file_content
            except FileNotFoundError:
                logger.info(f"File not found: {file_path}")
                continue

    if not submissions:
        logger.warning('No submissions found')
        return None

    sorted_submissions = dict(sorted(submissions.items()))
    return sorted_submissions


def download_from_s3(s3_bucket: str, s3_path: str, local_path: str):
    s3 = boto3.client('s3')
    try:
        s3.download_file(s3_bucket, s3_path, local_path)
    except Exception as e:
        raise Exception(f"Failed to download from S3: {e}")


def get_prompts(assignment: str) -> dict:
    s3_solutions_dir = f"academy/2/homework-keys/streaming-pipelines"
    local_solutions_dir = os.path.join(os.getcwd(), 'prompts', assignment)
    os.makedirs(local_solutions_dir, exist_ok=True)
    prompts = [
        'session_ddl.md',
        'session_job.md',
        'session_ddl.sql',
        'session_job.py'
    ]
    prompt_contents = {}
    for prompt in prompts:
        s3_path = f"{s3_solutions_dir}/{prompt}"
        logger.info(s3_path)
        local_path = os.path.join(local_solutions_dir, prompt)
        download_from_s3(s3_bucket, s3_path, local_path)
        if not os.path.exists(local_path):
            raise ValueError(f"File failed to download to {local_path}. Path does not exist.")
        with open(local_path, "r") as file:
            prompt_contents[prompt] = file.read()
    return prompt_contents


def generate_system_prompt():
    system_prompt = """
    You are a senior data engineer at an EdTech company, acting as a Teaching Assistant. Your tasks are twofold:
1. Provide detailed feedback on a student's homework submission based on the specified criteria.
2. Evaluate the solution and recommend a final grade using the provided grading rubric.

Your response will be added as a comment on the student's Pull Request.

### Assignment Instructions

The homework requires the student to:
- create one session_ddl.sql file for a session based table that is partitioned 
- create a session_job.py file that uses session_window, geocodes ip address with country, city, and state, and computes start and end window
    
    """

    return system_prompt


def generate_feedback_prompt(submissions: dict) -> str:
    user_prompt =  """
    ## Task: Provide Feedback

### Instructions

Evaluate the student's code based on the following criteria:
1. **Coverage of Homework Prompt**: Does the student's code address all aspects of the assignment? If any parts are missing or incomplete, provide specific examples or suggestions for improvement, referring to the example solution if needed.
2. **Logical Structure**: Does the student's solution logically follow the assignment instructions? Provide feedback on the overall structure and coherence of their approach.
3. **Readability**: Assess the readability of the code. Consider the use of meaningful variable names, inclusion of comments explaining the thought process, and overall formatting for readability.
4. **Robustness and Error Handling**: Evaluate how well the code handles edge cases, invalid inputs, and potential errors.
5. **Modularity and Reusability**: Assess whether the code is structured in a way that makes it easy to understand and reuse specific components.

Please ensure your feedback is positive, focused, and constructive, offering specific suggestions for improvement where necessary.
    
    """
    user_prompt += "\n\n"
    for file_name, submission in submissions.items():
        user_prompt += "# Student's Solution"
        user_prompt += f"Please analyze the following code in `{file_name}`:\n\n```\n{submission}\n```\n\n"
    return user_prompt


def generate_grading_prompt(submissions: dict) -> str:
    user_prompt = """
    ## Task: Provide a Grade

### Instructions

Evaluate the student's homework submission in the following areas: `Query Conversion` and `PySpark Jobs`. Assign a rating of **Proficient**, **Satisfactory**, **Needs Improvement**, or **Unsatisfactory** for each area. A passing grade requires at least "Satisfactory" in both areas.

### Grading Rubric

**Proficient**
session_ddl.sql: Well-structured. Has the necessary columns session_start, session_end, session_date, event_count, device_family, browser_family. And it is partitioned by session_date
session_job.py: Well-structured and error-free. Demonstrates robust Spark capabilities. Demonstrates understand of Spark Streaming and session_window function

**Satisfactory**
session_ddl.sql: Has the right structure but misses one of the critical columns
session_job.py: Demonstrates robust Spark Streaming capabilities. Demonstrates understanding of Spark Streaming and session_window function. Now errors there

**Needs Improvement**
session_ddl.sql: Misses critical dimensions in the group by
session_job.py: Misses critical dimensions in the group by

**Unsatisfactory**
session_ddl.sql: Missing too many columns, Does not sessionize. Code does not run 
session_job.py: Does not use session_window function. Code does not run 


**Example solution:**
```sql
CREATE TABLE IF NOT EXISTS <username>.dataexpert_sesions (
  host STRING,
  session_id STRING,
  user_id BIGINT,
  is_logged_in BOOLEAN,
  country STRING,
  state STRING,
  city STRING,
  browser_family STRING,
  device_family STRING,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  event_count BIGINT,
  session_date DATE
)
USING ICEBERG
PARTITIONED BY (session_date)
```

```python
import ast
import sys
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, hash, session_window, from_json, udf,unix_timestamp, coalesce
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructType, StructField, MapType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# TODO PUT YOUR API KEY HERE
GEOCODING_API_KEY = '<geocoding api key>'

def geocode_ip_address(ip_address):
    url = "https://api.ip2location.io"
    response = requests.get(url, params={
        'ip': ip_address,
        'key': GEOCODING_API_KEY
    })

    if response.status_code != 200:
        # Return empty dict if request failed
        return {}

    data = json.loads(response.text)

    # Extract the country and state from the response
    # This might change depending on the actual response structure
    country = data.get('country_code', '')
    state = data.get('region_name', '')
    city = data.get('city_name', '')

    return {'country': country, 'state': state, 'city': city}

spark = (SparkSession.builder
         .getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME",
                                     "ds",
                                     'output_table',
                                     'kafka_credentials',
                                     'checkpoint_location'
                                     ])
run_date = args['ds']
output_table = args['output_table']
checkpoint_location = args['checkpoint_location']
kafka_credentials = ast.literal_eval(args['kafka_credentials'])
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# Retrieve Kafka credentials from environment variables
kafka_key = kafka_credentials['KAFKA_WEB_TRAFFIC_KEY']
kafka_secret = kafka_credentials['KAFKA_WEB_TRAFFIC_SECRET']
kafka_bootstrap_servers = kafka_credentials['KAFKA_WEB_BOOTSTRAP_SERVER']
kafka_topic = kafka_credentials['KAFKA_TOPIC']

if kafka_key is None or kafka_secret is None:
    raise ValueError("KAFKA_WEB_TRAFFIC_KEY and KAFKA_WEB_TRAFFIC_SECRET must be set as environment variables.")

# Kafka configuration

start_timestamp = f"{run_date}T00:00:00.000Z"

# Define the schema of the Kafka message value
schema = StructType([
    StructField("url", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("user_agent", StructType([
        StructField("family", StringType(), True),
        StructField("major", StringType(), True),
        StructField("minor", StringType(), True),
        StructField("patch", StringType(), True),
        StructField("device", StructType([
            StructField("family", StringType(), True),
            StructField("major", StringType(), True),
            StructField("minor", StringType(), True),
            StructField("patch", StringType(), True),
        ]), True),
        StructField("os", StructType([
            StructField("family", StringType(), True),
            StructField("major", StringType(), True),
            StructField("minor", StringType(), True),
            StructField("patch", StringType(), True),
        ]), True)
    ]), True),
    StructField("headers", MapType(keyType=StringType(), valueType=StringType()), True),
    StructField("host", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("academy_id", IntegerType(), True),
    StructField("event_time", TimestampType(), True)
])


spark.sql(f
CREATE TABLE IF NOT EXISTS {output_table} (
  host STRING,
  session_id STRING,
  user_id BIGINT,
  is_logged_in BOOLEAN,
  country STRING,
  state STRING,
  city STRING,
  browser_family STRING,
  device_family STRING,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  event_count BIGINT,
  session_date DATE
)
USING ICEBERG
PARTITIONED BY (session_date)

# Read from Kafka in batch mode
kafka_df = (spark 
    .readStream 
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 10000) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";') \
    .load()
)

def decode_col(column):
    return column.decode('utf-8')

decode_udf = udf(decode_col, StringType())

geocode_schema = StructType([
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
])

geocode_udf = udf(geocode_ip_address, geocode_schema)

tumbling_window_df = kafka_df \
    .withColumn("decoded_value", decode_udf(col("value"))) \
    .withColumn("value", from_json(col("decoded_value"), schema)) \
    .withColumn("geodata", geocode_udf(col("value.ip"))) \
    .withWatermark("timestamp", "30 seconds")

by_country = tumbling_window_df.groupBy(session_window(col("timestamp"), "5 minutes"),
                                        col("value.ip"),
                                        col("value.host"),
                                        col("value.user_id"),
                                        col("geodata.country"),
                                        col("geodata.state"),
                                        col("geodata.city"),
                                        col("value.user_agent.family").alias("browser_family"),
                                        col("value.user_agent.device.family").alias("device_family")
                                        ) \
    .count() \
    .select(
        col("host"),
        hash(col("ip"), unix_timestamp(col("session_window.start")).cast("string"), coalesce(
            col("user_id").cast("string"), lit('logged_out'))).cast("string").alias("session_id"),
        col("user_id"),
        col("user_id").isNotNull().alias("is_logged_in"),
        col("country"),
        col("state"),
        col("city"),
        col("browser_family"),
        col("device_family"),
        col("session_window.start").alias("window_start"),
        col("session_window.end").alias("window_end"),
        col("count").alias("event_count"),
        col("session_window.start").cast("date").alias("session_date")
    )

query = by_country \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("fanout-enabled", "true") \
    .option("checkpointLocation", checkpoint_location) \
    .toTable(output_table)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# stop the job after 60 minutes
# PLEASE DO NOT REMOVE TIMEOUT
query.awaitTermination(timeout=60*60)

### Final Grade

Based on the evaluations above, recommend a final grade and a brief summary of the assessment."""
    user_prompt += "\n\n"

    # user_prompt += "# Additional Information"
    # user_prompt += f"\n\n{prompts['week_1_queries.md']}"
    # user_prompt += f"\n\n{prompts['week_2_queries.md']}"
    # user_prompt += f"\n\n{prompts['example_solution.md']}"
    # user_prompt += "\n\n"

    for file_name, submission in submissions.items():
        user_prompt += "# Student's Solution"
        user_prompt += f"Please grade the following code in `{file_name}`:\n\n```\n{submission}\n```\n\n"
    return user_prompt


def get_response(system_prompt: str, user_prompt: str) -> str:
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1
    )
    comment = response.choices[0].message.content
    # text = f"This is a LLM-generated comment: \n{comment if comment else 'No feedback generated.'}"
    return comment


def post_github_comment(git_token, repo, pr_number, comment):
    url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {git_token}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    data = {"body": comment}
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 201:
        logger.error(f"Failed to create comment. Status code: {response.status_code} \n{response.text}")
        raise Exception(f"Failed to create comment. Status code: {response.status_code} \n{response.text}")
    logger.info(f"✅ Added review comment at https://github.com/{repo}/pull/{pr_number}")


def main():
    submissions = get_submissions(submission_dir)
    if not submissions:
        logger.warning(
            f"No comments were generated because no files were found in the `{submission_dir}` directory. Please modify one or more of the files at `src/jobs/` or `src/tests/` to receive LLM-generated feedback.")
        return None

    assignment = get_assignment()
    system_prompt = generate_system_prompt()

    feedback_prompt = generate_feedback_prompt(submissions)
    feedback_comment = get_response(system_prompt, feedback_prompt)

    grading_prompt = generate_grading_prompt(submissions)
    grading_comment = get_response(system_prompt, grading_prompt)

    final_comment = f"### Feedback:\n{feedback_comment}\n\n### Grade:\n{grading_comment}"

    if git_token and repo and pr_number:
        post_github_comment(git_token, repo, pr_number, final_comment)

    return final_comment


if __name__ == "__main__":
    main()
