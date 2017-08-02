import api_calls
import json
import pandas as pd


# Metadata
if __name__ == "__main__":

    # Pull in credentials
    with open("credentials.json", encoding="utf-8") as data_file:
        credentials = json.load(data_file)

    CLIENT_ID = credentials['client_id']
    SECRET = credentials['secret']
    ACCESS_TOKEN = credentials['access_token']

    survey_id = 120134627

    # Create API wrapper instance
    test = api_calls.SurveyResults(ACCESS_TOKEN)

    # Create dfs
    test_details = test.get_survey_details(survey_id)
    test_responses = test.get_survey_responses(survey_id)

    # Long format
    merged = pd.merge(test_details, test_responses,
                      left_on=['question_id', 'choice_id', 'survey_id', 'rows_id'],
                      right_on=['question_id', 'choice_id', 'survey_id', 'row_id'])
