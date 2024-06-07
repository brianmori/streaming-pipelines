import os
import re
import base64

import logging
import requests
import boto3
from pydantic import Base64Encoder

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
    user_prompt = "IyMgVGFzazogUHJvdmlkZSBhIEdyYWRlCgojIyMgSW5zdHJ1Y3Rpb25zCgpFdmFsdWF0ZSB0aGUgc3R1ZGVudCdzIGhvbWV3b3JrIHN1Ym1pc3Npb24gaW4gdGhlIGZvbGxvd2luZyBhcmVhczogYFF1ZXJ5IENvbnZlcnNpb25gIGFuZCBgUHlTcGFyayBKb2JzYC4gQXNzaWduIGEgcmF0aW5nIG9mICoqUHJvZmljaWVudCoqLCAqKlNhdGlzZmFjdG9yeSoqLCAqKk5lZWRzIEltcHJvdmVtZW50KiosIG9yICoqVW5zYXRpc2ZhY3RvcnkqKiBmb3IgZWFjaCBhcmVhLiBBIHBhc3NpbmcgZ3JhZGUgcmVxdWlyZXMgYXQgbGVhc3QgIlNhdGlzZmFjdG9yeSIgaW4gYm90aCBhcmVhcy4KCiMjIyBHcmFkaW5nIFJ1YnJpYwoKKipQcm9maWNpZW50KioKc2Vzc2lvbl9kZGwuc3FsOiBXZWxsLXN0cnVjdHVyZWQuIEhhcyB0aGUgbmVjZXNzYXJ5IGNvbHVtbnMgc2Vzc2lvbl9zdGFydCwgc2Vzc2lvbl9lbmQsIHNlc3Npb25fZGF0ZSwgZXZlbnRfY291bnQsIGRldmljZV9mYW1pbHksIGJyb3dzZXJfZmFtaWx5LiBBbmQgaXQgaXMgcGFydGl0aW9uZWQgYnkgc2Vzc2lvbl9kYXRlCnNlc3Npb25fam9iLnB5OiBXZWxsLXN0cnVjdHVyZWQgYW5kIGVycm9yLWZyZWUuIERlbW9uc3RyYXRlcyByb2J1c3QgU3BhcmsgY2FwYWJpbGl0aWVzLiBEZW1vbnN0cmF0ZXMgdW5kZXJzdGFuZCBvZiBTcGFyayBTdHJlYW1pbmcgYW5kIHNlc3Npb25fd2luZG93IGZ1bmN0aW9uCgoqKlNhdGlzZmFjdG9yeSoqCnNlc3Npb25fZGRsLnNxbDogSGFzIHRoZSByaWdodCBzdHJ1Y3R1cmUgYnV0IG1pc3NlcyBvbmUgb2YgdGhlIGNyaXRpY2FsIGNvbHVtbnMKc2Vzc2lvbl9qb2IucHk6IERlbW9uc3RyYXRlcyByb2J1c3QgU3BhcmsgU3RyZWFtaW5nIGNhcGFiaWxpdGllcy4gRGVtb25zdHJhdGVzIHVuZGVyc3RhbmRpbmcgb2YgU3BhcmsgU3RyZWFtaW5nIGFuZCBzZXNzaW9uX3dpbmRvdyBmdW5jdGlvbi4gTm93IGVycm9ycyB0aGVyZQoKKipOZWVkcyBJbXByb3ZlbWVudCoqCnNlc3Npb25fZGRsLnNxbDogTWlzc2VzIGNyaXRpY2FsIGRpbWVuc2lvbnMgaW4gdGhlIGdyb3VwIGJ5CnNlc3Npb25fam9iLnB5OiBNaXNzZXMgY3JpdGljYWwgZGltZW5zaW9ucyBpbiB0aGUgZ3JvdXAgYnkKCioqVW5zYXRpc2ZhY3RvcnkqKgpzZXNzaW9uX2RkbC5zcWw6IE1pc3NpbmcgdG9vIG1hbnkgY29sdW1ucywgRG9lcyBub3Qgc2Vzc2lvbml6ZS4gQ29kZSBkb2VzIG5vdCBydW4gCnNlc3Npb25fam9iLnB5OiBEb2VzIG5vdCB1c2Ugc2Vzc2lvbl93aW5kb3cgZnVuY3Rpb24uIENvZGUgZG9lcyBub3QgcnVuIAoKCioqRXhhbXBsZSBzb2x1dGlvbjoqKgpgYGBzcWwKQ1JFQVRFIFRBQkxFIElGIE5PVCBFWElTVFMgPHVzZXJuYW1lPi5kYXRhZXhwZXJ0X3Nlc2lvbnMgKAogIGhvc3QgU1RSSU5HLAogIHNlc3Npb25faWQgU1RSSU5HLAogIHVzZXJfaWQgQklHSU5ULAogIGlzX2xvZ2dlZF9pbiBCT09MRUFOLAogIGNvdW50cnkgU1RSSU5HLAogIHN0YXRlIFNUUklORywKICBjaXR5IFNUUklORywKICBicm93c2VyX2ZhbWlseSBTVFJJTkcsCiAgZGV2aWNlX2ZhbWlseSBTVFJJTkcsCiAgd2luZG93X3N0YXJ0IFRJTUVTVEFNUCwKICB3aW5kb3dfZW5kIFRJTUVTVEFNUCwKICBldmVudF9jb3VudCBCSUdJTlQsCiAgc2Vzc2lvbl9kYXRlIERBVEUKKQpVU0lORyBJQ0VCRVJHClBBUlRJVElPTkVEIEJZIChzZXNzaW9uX2RhdGUpCmBgYAoKYGBgcHl0aG9uCmltcG9ydCBhc3QKaW1wb3J0IHN5cwppbXBvcnQgcmVxdWVzdHMKaW1wb3J0IGpzb24KZnJvbSBweXNwYXJrLnNxbCBpbXBvcnQgU3BhcmtTZXNzaW9uCmZyb20gcHlzcGFyay5zcWwuZnVuY3Rpb25zIGltcG9ydCBjb2wsIGxpdCwgaGFzaCwgc2Vzc2lvbl93aW5kb3csIGZyb21fanNvbiwgdWRmLHVuaXhfdGltZXN0YW1wLCBjb2FsZXNjZQpmcm9tIHB5c3Bhcmsuc3FsLnR5cGVzIGltcG9ydCBTdHJpbmdUeXBlLCBJbnRlZ2VyVHlwZSwgVGltZXN0YW1wVHlwZSwgU3RydWN0VHlwZSwgU3RydWN0RmllbGQsIE1hcFR5cGUKZnJvbSBhd3NnbHVlLnV0aWxzIGltcG9ydCBnZXRSZXNvbHZlZE9wdGlvbnMKZnJvbSBhd3NnbHVlLmNvbnRleHQgaW1wb3J0IEdsdWVDb250ZXh0CmZyb20gYXdzZ2x1ZS5qb2IgaW1wb3J0IEpvYgoKIyBUT0RPIFBVVCBZT1VSIEFQSSBLRVkgSEVSRQpHRU9DT0RJTkdfQVBJX0tFWSA9ICc8Z2VvY29kaW5nIGFwaSBrZXk+JwoKZGVmIGdlb2NvZGVfaXBfYWRkcmVzcyhpcF9hZGRyZXNzKToKICAgIHVybCA9ICJodHRwczovL2FwaS5pcDJsb2NhdGlvbi5pbyIKICAgIHJlc3BvbnNlID0gcmVxdWVzdHMuZ2V0KHVybCwgcGFyYW1zPXsKICAgICAgICAnaXAnOiBpcF9hZGRyZXNzLAogICAgICAgICdrZXknOiBHRU9DT0RJTkdfQVBJX0tFWQogICAgfSkKCiAgICBpZiByZXNwb25zZS5zdGF0dXNfY29kZSAhPSAyMDA6CiAgICAgICAgIyBSZXR1cm4gZW1wdHkgZGljdCBpZiByZXF1ZXN0IGZhaWxlZAogICAgICAgIHJldHVybiB7fQoKICAgIGRhdGEgPSBqc29uLmxvYWRzKHJlc3BvbnNlLnRleHQpCgogICAgIyBFeHRyYWN0IHRoZSBjb3VudHJ5IGFuZCBzdGF0ZSBmcm9tIHRoZSByZXNwb25zZQogICAgIyBUaGlzIG1pZ2h0IGNoYW5nZSBkZXBlbmRpbmcgb24gdGhlIGFjdHVhbCByZXNwb25zZSBzdHJ1Y3R1cmUKICAgIGNvdW50cnkgPSBkYXRhLmdldCgnY291bnRyeV9jb2RlJywgJycpCiAgICBzdGF0ZSA9IGRhdGEuZ2V0KCdyZWdpb25fbmFtZScsICcnKQogICAgY2l0eSA9IGRhdGEuZ2V0KCdjaXR5X25hbWUnLCAnJykKCiAgICByZXR1cm4geydjb3VudHJ5JzogY291bnRyeSwgJ3N0YXRlJzogc3RhdGUsICdjaXR5JzogY2l0eX0KCnNwYXJrID0gKFNwYXJrU2Vzc2lvbi5idWlsZGVyCiAgICAgICAgIC5nZXRPckNyZWF0ZSgpKQphcmdzID0gZ2V0UmVzb2x2ZWRPcHRpb25zKHN5cy5hcmd2LCBbIkpPQl9OQU1FIiwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJkcyIsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAnb3V0cHV0X3RhYmxlJywKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICdrYWZrYV9jcmVkZW50aWFscycsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAnY2hlY2twb2ludF9sb2NhdGlvbicKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIF0pCnJ1bl9kYXRlID0gYXJnc1snZHMnXQpvdXRwdXRfdGFibGUgPSBhcmdzWydvdXRwdXRfdGFibGUnXQpjaGVja3BvaW50X2xvY2F0aW9uID0gYXJnc1snY2hlY2twb2ludF9sb2NhdGlvbiddCmthZmthX2NyZWRlbnRpYWxzID0gYXN0LmxpdGVyYWxfZXZhbChhcmdzWydrYWZrYV9jcmVkZW50aWFscyddKQpnbHVlQ29udGV4dCA9IEdsdWVDb250ZXh0KHNwYXJrLnNwYXJrQ29udGV4dCkKc3BhcmsgPSBnbHVlQ29udGV4dC5zcGFya19zZXNzaW9uCgojIFJldHJpZXZlIEthZmthIGNyZWRlbnRpYWxzIGZyb20gZW52aXJvbm1lbnQgdmFyaWFibGVzCmthZmthX2tleSA9IGthZmthX2NyZWRlbnRpYWxzWydLQUZLQV9XRUJfVFJBRkZJQ19LRVknXQprYWZrYV9zZWNyZXQgPSBrYWZrYV9jcmVkZW50aWFsc1snS0FGS0FfV0VCX1RSQUZGSUNfU0VDUkVUJ10Ka2Fma2FfYm9vdHN0cmFwX3NlcnZlcnMgPSBrYWZrYV9jcmVkZW50aWFsc1snS0FGS0FfV0VCX0JPT1RTVFJBUF9TRVJWRVInXQprYWZrYV90b3BpYyA9IGthZmthX2NyZWRlbnRpYWxzWydLQUZLQV9UT1BJQyddCgppZiBrYWZrYV9rZXkgaXMgTm9uZSBvciBrYWZrYV9zZWNyZXQgaXMgTm9uZToKICAgIHJhaXNlIFZhbHVlRXJyb3IoIktBRktBX1dFQl9UUkFGRklDX0tFWSBhbmQgS0FGS0FfV0VCX1RSQUZGSUNfU0VDUkVUIG11c3QgYmUgc2V0IGFzIGVudmlyb25tZW50IHZhcmlhYmxlcy4iKQoKIyBLYWZrYSBjb25maWd1cmF0aW9uCgpzdGFydF90aW1lc3RhbXAgPSBmIntydW5fZGF0ZX1UMDA6MDA6MDAuMDAwWiIKCiMgRGVmaW5lIHRoZSBzY2hlbWEgb2YgdGhlIEthZmthIG1lc3NhZ2UgdmFsdWUKc2NoZW1hID0gU3RydWN0VHlwZShbCiAgICBTdHJ1Y3RGaWVsZCgidXJsIiwgU3RyaW5nVHlwZSgpLCBUcnVlKSwKICAgIFN0cnVjdEZpZWxkKCJyZWZlcnJlciIsIFN0cmluZ1R5cGUoKSwgVHJ1ZSksCiAgICBTdHJ1Y3RGaWVsZCgidXNlcl9hZ2VudCIsIFN0cnVjdFR5cGUoWwogICAgICAgIFN0cnVjdEZpZWxkKCJmYW1pbHkiLCBTdHJpbmdUeXBlKCksIFRydWUpLAogICAgICAgIFN0cnVjdEZpZWxkKCJtYWpvciIsIFN0cmluZ1R5cGUoKSwgVHJ1ZSksCiAgICAgICAgU3RydWN0RmllbGQoIm1pbm9yIiwgU3RyaW5nVHlwZSgpLCBUcnVlKSwKICAgICAgICBTdHJ1Y3RGaWVsZCgicGF0Y2giLCBTdHJpbmdUeXBlKCksIFRydWUpLAogICAgICAgIFN0cnVjdEZpZWxkKCJkZXZpY2UiLCBTdHJ1Y3RUeXBlKFsKICAgICAgICAgICAgU3RydWN0RmllbGQoImZhbWlseSIsIFN0cmluZ1R5cGUoKSwgVHJ1ZSksCiAgICAgICAgICAgIFN0cnVjdEZpZWxkKCJtYWpvciIsIFN0cmluZ1R5cGUoKSwgVHJ1ZSksCiAgICAgICAgICAgIFN0cnVjdEZpZWxkKCJtaW5vciIsIFN0cmluZ1R5cGUoKSwgVHJ1ZSksCiAgICAgICAgICAgIFN0cnVjdEZpZWxkKCJwYXRjaCIsIFN0cmluZ1R5cGUoKSwgVHJ1ZSksCiAgICAgICAgXSksIFRydWUpLAogICAgICAgIFN0cnVjdEZpZWxkKCJvcyIsIFN0cnVjdFR5cGUoWwogICAgICAgICAgICBTdHJ1Y3RGaWVsZCgiZmFtaWx5IiwgU3RyaW5nVHlwZSgpLCBUcnVlKSwKICAgICAgICAgICAgU3RydWN0RmllbGQoIm1ham9yIiwgU3RyaW5nVHlwZSgpLCBUcnVlKSwKICAgICAgICAgICAgU3RydWN0RmllbGQoIm1pbm9yIiwgU3RyaW5nVHlwZSgpLCBUcnVlKSwKICAgICAgICAgICAgU3RydWN0RmllbGQoInBhdGNoIiwgU3RyaW5nVHlwZSgpLCBUcnVlKSwKICAgICAgICBdKSwgVHJ1ZSkKICAgIF0pLCBUcnVlKSwKICAgIFN0cnVjdEZpZWxkKCJoZWFkZXJzIiwgTWFwVHlwZShrZXlUeXBlPVN0cmluZ1R5cGUoKSwgdmFsdWVUeXBlPVN0cmluZ1R5cGUoKSksIFRydWUpLAogICAgU3RydWN0RmllbGQoImhvc3QiLCBTdHJpbmdUeXBlKCksIFRydWUpLAogICAgU3RydWN0RmllbGQoImlwIiwgU3RyaW5nVHlwZSgpLCBUcnVlKSwKICAgIFN0cnVjdEZpZWxkKCJ1c2VyX2lkIiwgSW50ZWdlclR5cGUoKSwgVHJ1ZSksCiAgICBTdHJ1Y3RGaWVsZCgiYWNhZGVteV9pZCIsIEludGVnZXJUeXBlKCksIFRydWUpLAogICAgU3RydWN0RmllbGQoImV2ZW50X3RpbWUiLCBUaW1lc3RhbXBUeXBlKCksIFRydWUpCl0pCgoKc3Bhcmsuc3FsKGYKQ1JFQVRFIFRBQkxFIElGIE5PVCBFWElTVFMge291dHB1dF90YWJsZX0gKAogIGhvc3QgU1RSSU5HLAogIHNlc3Npb25faWQgU1RSSU5HLAogIHVzZXJfaWQgQklHSU5ULAogIGlzX2xvZ2dlZF9pbiBCT09MRUFOLAogIGNvdW50cnkgU1RSSU5HLAogIHN0YXRlIFNUUklORywKICBjaXR5IFNUUklORywKICBicm93c2VyX2ZhbWlseSBTVFJJTkcsCiAgZGV2aWNlX2ZhbWlseSBTVFJJTkcsCiAgd2luZG93X3N0YXJ0IFRJTUVTVEFNUCwKICB3aW5kb3dfZW5kIFRJTUVTVEFNUCwKICBldmVudF9jb3VudCBCSUdJTlQsCiAgc2Vzc2lvbl9kYXRlIERBVEUKKQpVU0lORyBJQ0VCRVJHClBBUlRJVElPTkVEIEJZIChzZXNzaW9uX2RhdGUpCgojIFJlYWQgZnJvbSBLYWZrYSBpbiBiYXRjaCBtb2RlCmthZmthX2RmID0gKHNwYXJrIAogICAgLnJlYWRTdHJlYW0gCiAgICAuZm9ybWF0KCJrYWZrYSIpICAgICAub3B0aW9uKCJrYWZrYS5ib290c3RyYXAuc2VydmVycyIsIGthZmthX2Jvb3RzdHJhcF9zZXJ2ZXJzKSAgICAgLm9wdGlvbigic3Vic2NyaWJlIiwga2Fma2FfdG9waWMpICAgICAub3B0aW9uKCJzdGFydGluZ09mZnNldHMiLCAibGF0ZXN0IikgICAgIC5vcHRpb24oIm1heE9mZnNldHNQZXJUcmlnZ2VyIiwgMTAwMDApICAgICAub3B0aW9uKCJrYWZrYS5zZWN1cml0eS5wcm90b2NvbCIsICJTQVNMX1NTTCIpICAgICAub3B0aW9uKCJrYWZrYS5zYXNsLm1lY2hhbmlzbSIsICJQTEFJTiIpICAgICAub3B0aW9uKCJrYWZrYS5zYXNsLmphYXMuY29uZmlnIiwKICAgICAgICAgICAgZidvcmcuYXBhY2hlLmthZmthLmNvbW1vbi5zZWN1cml0eS5wbGFpbi5QbGFpbkxvZ2luTW9kdWxlIHJlcXVpcmVkIHVzZXJuYW1lPSJ7a2Fma2Ffa2V5fSIgcGFzc3dvcmQ9IntrYWZrYV9zZWNyZXR9IjsnKSAgICAgLmxvYWQoKQopCgpkZWYgZGVjb2RlX2NvbChjb2x1bW4pOgogICAgcmV0dXJuIGNvbHVtbi5kZWNvZGUoJ3V0Zi04JykKCmRlY29kZV91ZGYgPSB1ZGYoZGVjb2RlX2NvbCwgU3RyaW5nVHlwZSgpKQoKZ2VvY29kZV9zY2hlbWEgPSBTdHJ1Y3RUeXBlKFsKICAgIFN0cnVjdEZpZWxkKCJjb3VudHJ5IiwgU3RyaW5nVHlwZSgpLCBUcnVlKSwKICAgIFN0cnVjdEZpZWxkKCJjaXR5IiwgU3RyaW5nVHlwZSgpLCBUcnVlKSwKICAgIFN0cnVjdEZpZWxkKCJzdGF0ZSIsIFN0cmluZ1R5cGUoKSwgVHJ1ZSksCl0pCgpnZW9jb2RlX3VkZiA9IHVkZihnZW9jb2RlX2lwX2FkZHJlc3MsIGdlb2NvZGVfc2NoZW1hKQoKdHVtYmxpbmdfd2luZG93X2RmID0ga2Fma2FfZGYgICAgIC53aXRoQ29sdW1uKCJkZWNvZGVkX3ZhbHVlIiwgZGVjb2RlX3VkZihjb2woInZhbHVlIikpKSAgICAgLndpdGhDb2x1bW4oInZhbHVlIiwgZnJvbV9qc29uKGNvbCgiZGVjb2RlZF92YWx1ZSIpLCBzY2hlbWEpKSAgICAgLndpdGhDb2x1bW4oImdlb2RhdGEiLCBnZW9jb2RlX3VkZihjb2woInZhbHVlLmlwIikpKSAgICAgLndpdGhXYXRlcm1hcmsoInRpbWVzdGFtcCIsICIzMCBzZWNvbmRzIikKCmJ5X2NvdW50cnkgPSB0dW1ibGluZ193aW5kb3dfZGYuZ3JvdXBCeShzZXNzaW9uX3dpbmRvdyhjb2woInRpbWVzdGFtcCIpLCAiNSBtaW51dGVzIiksCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBjb2woInZhbHVlLmlwIiksCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBjb2woInZhbHVlLmhvc3QiKSwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNvbCgidmFsdWUudXNlcl9pZCIpLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgY29sKCJnZW9kYXRhLmNvdW50cnkiKSwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNvbCgiZ2VvZGF0YS5zdGF0ZSIpLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgY29sKCJnZW9kYXRhLmNpdHkiKSwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNvbCgidmFsdWUudXNlcl9hZ2VudC5mYW1pbHkiKS5hbGlhcygiYnJvd3Nlcl9mYW1pbHkiKSwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNvbCgidmFsdWUudXNlcl9hZ2VudC5kZXZpY2UuZmFtaWx5IikuYWxpYXMoImRldmljZV9mYW1pbHkiKQogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgKSAgICAgLmNvdW50KCkgICAgIC5zZWxlY3QoCiAgICAgICAgY29sKCJob3N0IiksCiAgICAgICAgaGFzaChjb2woImlwIiksIHVuaXhfdGltZXN0YW1wKGNvbCgic2Vzc2lvbl93aW5kb3cuc3RhcnQiKSkuY2FzdCgic3RyaW5nIiksIGNvYWxlc2NlKAogICAgICAgICAgICBjb2woInVzZXJfaWQiKS5jYXN0KCJzdHJpbmciKSwgbGl0KCdsb2dnZWRfb3V0JykpKS5jYXN0KCJzdHJpbmciKS5hbGlhcygic2Vzc2lvbl9pZCIpLAogICAgICAgIGNvbCgidXNlcl9pZCIpLAogICAgICAgIGNvbCgidXNlcl9pZCIpLmlzTm90TnVsbCgpLmFsaWFzKCJpc19sb2dnZWRfaW4iKSwKICAgICAgICBjb2woImNvdW50cnkiKSwKICAgICAgICBjb2woInN0YXRlIiksCiAgICAgICAgY29sKCJjaXR5IiksCiAgICAgICAgY29sKCJicm93c2VyX2ZhbWlseSIpLAogICAgICAgIGNvbCgiZGV2aWNlX2ZhbWlseSIpLAogICAgICAgIGNvbCgic2Vzc2lvbl93aW5kb3cuc3RhcnQiKS5hbGlhcygid2luZG93X3N0YXJ0IiksCiAgICAgICAgY29sKCJzZXNzaW9uX3dpbmRvdy5lbmQiKS5hbGlhcygid2luZG93X2VuZCIpLAogICAgICAgIGNvbCgiY291bnQiKS5hbGlhcygiZXZlbnRfY291bnQiKSwKICAgICAgICBjb2woInNlc3Npb25fd2luZG93LnN0YXJ0IikuY2FzdCgiZGF0ZSIpLmFsaWFzKCJzZXNzaW9uX2RhdGUiKQogICAgKQoKcXVlcnkgPSBieV9jb3VudHJ5ICAgICAud3JpdGVTdHJlYW0gICAgIC5mb3JtYXQoImljZWJlcmciKSAgICAgLm91dHB1dE1vZGUoImFwcGVuZCIpICAgICAudHJpZ2dlcihwcm9jZXNzaW5nVGltZT0iNSBzZWNvbmRzIikgICAgIC5vcHRpb24oImZhbm91dC1lbmFibGVkIiwgInRydWUiKSAgICAgLm9wdGlvbigiY2hlY2twb2ludExvY2F0aW9uIiwgY2hlY2twb2ludF9sb2NhdGlvbikgICAgIC50b1RhYmxlKG91dHB1dF90YWJsZSkKCmpvYiA9IEpvYihnbHVlQ29udGV4dCkKam9iLmluaXQoYXJnc1siSk9CX05BTUUiXSwgYXJncykKCiMgc3RvcCB0aGUgam9iIGFmdGVyIDYwIG1pbnV0ZXMKIyBQTEVBU0UgRE8gTk9UIFJFTU9WRSBUSU1FT1VUCnF1ZXJ5LmF3YWl0VGVybWluYXRpb24odGltZW91dD02MCo2MCkKCiMjIyBGaW5hbCBHcmFkZQoKQmFzZWQgb24gdGhlIGV2YWx1YXRpb25zIGFib3ZlLCByZWNvbW1lbmQgYSBmaW5hbCBncmFkZSBhbmQgYSBicmllZiBzdW1tYXJ5IG9mIHRoZSBhc3Nlc3NtZW50Lg=="
    decoded = base64.b64decode(user_prompt.encode('utf-8'))
    user_prompt = decoded.decode('utf-8')
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
        model="gpt-4o",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1
    )
    comment = response.choices[0].message.content
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
