#!/usr/bin/env bash
#Pass only first 5 arguments to test a client call for a new pipeline run to the locally running application.
#Grab the pipeline_run_id from the response of that initial call to add as a 6th parameter, which would then return the current status of that pipeline run.
source=$1
report_type=$2
start_date=$3
end_date=$4
path=$5
id=$6

#None of these are valid secrets, but headers do need to be passed in for local testing
key='x-api-key: qwerty'
content_type='Content-type: application/json'
member_of='x-tgt-memberof: CN=APP-OAUTH2-MDF-NPE'
auth='Authorization: Bearer qwerty'
url="http://localhost:8080/marketing_ingest_pipelines/v2/"


if [ -z "${id}" ]; then
 # shellcheck disable=SC2089
 payload="{\"source\":\"${source}\",\"report_type\":\"${report_type}\",\"start_date\":\"${start_date}\",\"end_date\":\"${end_date}\",\"target_path\":\"${path}\"}"
 curl -d "${payload}" -H "${key}" -H "${content_type}" -H "${member_of}" -H "${auth}" "$url"
else
  curl -H "${key}" -H "${content_type}" -H "${member_of}" -H "${auth}" "${url}${id}"
fi