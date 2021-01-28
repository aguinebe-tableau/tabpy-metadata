# os is used to retrieve environment variables
# requests is used to make HTTP requests out to Tableau Server
# json is used to parse the answer received from the Tableau metadata API
# pandas is used to build a dataframe and return data to Tableau Prep

import os
import requests
import json
import pandas as pd


# login will perform a login into Tableau Server and return a session token
# it uses the API_SERVER, API_SECRET_TOKEN and API_TOKEN_NAME environment variables
# see docker-compose.yml for these environment variables

def login():
    body = {"credentials": {"personalAccessTokenName": os.environ['API_TOKEN_NAME'],
                            "personalAccessTokenSecret": os.environ['API_SECRET_TOKEN'],
                            "site": {"contentUrl": ""}}}

    print("Performing login")

    url = "https://{server}/api/3.9/auth/signin".format(
        server=os.environ["API_SERVER"])

    headers = {
        'accept': 'application/json',
        'content-type': 'application/json'
    }

    # making the request
    x = requests.post(url, json=body, headers=headers)
    # parsing the login response
    response = json.loads(x.text)
    # if everything goes well, session token is found here:
    token = response["credentials"]["token"]

    return token

# run_query runs a GRAPHQL query against the Tableau metadata API
# arguments are a session token (obtained with login function) and the query text
# it uses the API_SERVER environment variable to connect to the server; see docker-compose.yml


def run_query(token, query):
    uri = "https://{server}/api/metadata/graphql".format(
        server=os.environ["API_SERVER"])
    headers = {
        'content-type': 'application/json',
        'accept': 'application/json',
        'X-Tableau-Auth': token
    }
    x = requests.post(uri, json={'query': query}, headers=headers)

    # we print the plain text answer for debugging purposes
    print(x.text)

    # we return the plain text answer received
    return x.text


# This function is called by Tableau Prep to obtain the expected schema of the data to receive
def get_output_schema():
    return pd.DataFrame({
        'ds_name': prep_string(),
        'flow_name': prep_string(),
        'owner_name': prep_string(),
        'project_name': prep_string()
    })


# This is the function we well name in a Tableau Prep script node
# It received data on the input, but doesn't use it

def get_published_ds_used_in_flow(input):

    # The hardcoded Graphql query
    query = """query published_datasources_certified {
    publishedDatasources {
      name
      isCertified
      downstreamFlows {
        name,
        owner {
          name
        },
        projectName
      }
    }
  }
  """

    print("Logging into Tableau Server...")
    token = login()
    print("Session token is: " + token)
    print("")
    print("Running the following query:")
    print(query)
    print("")
    print("Answer below:")

    # here we get text
    json_string = run_query(token, query)

    # we parse the result as a json structure
    response_as_json = json.loads(json_string)

    # we print the json structure
    print(json.dumps(response_as_json, sort_keys=True, indent=4))

    # navigating the json structure to find the starting node we need
    list_of_published_ds = response_as_json["data"]["publishedDatasources"]

    # this array will contain the data we will return to Tableau Prep
    resultset = []

    # navigating the json structure and collecting data
    for ds in list_of_published_ds:
        for flow in ds["downstreamFlows"]:
            # when we find a downstream flow, we create one entry into the resultset
            resultset.append([ds["name"], flow["name"],
                              flow["owner"]["name"], flow["projectName"]])

    # print the resultset for debugging purposes
    print(resultset)

    # we turn this python array into a pandas dataframe
    df = pd.DataFrame(data=resultset, columns=[
                      "ds_name", "flow_name", "owner_name", "project_name"])

    # we return the dataframe to Tableau Prep
    return(df)
