import requests
import json
import pandas as pd
from pandas.io.json import json_normalize


class ApiGetCall(object):
    def __init__(self, access_token):
        self.host = "https://api.surveymonkey.net/v3/"
        self.client = requests.session()
        self.client.headers = {
            "Authorization": "Bearer %s" % access_token,
            "Content-Type": "application/json"
        }

    def get_data(self, url):
        results = self.client.get(url)
        return results


class SurveyResults(ApiGetCall):
    def __init__(self, access_token):
        super().__init__(access_token)

    def get_survey_details(self, survey_id):
        """ Make a call to get survey details """
        url = self.host + "surveys/%s/details" % (survey_id)
        response = self.client.get(url).json()

        # Loop through pages to get question details
        data = response['pages']
        questions = []

        for page in data:
            page_data = self._parse_survey_details(page)
            if page_data is None:
                next
            else:
                questions.extend(page_data)

        # Compile and tack on survey metadata
        output = (pd.concat(questions).reset_index(drop=True))
        output['title'] = response['title']
        output['survey_id'] = survey_id
        output.columns = list(map(lambda x: x.replace('choices', 'choice'), output.columns))

        for col in output.columns:
            if "_id" in col:
                output[col] = output[col].astype(str)

        return output

    def _parse_survey_details(self, survey_detail_page):
        """ Helper function to get questions from page object """

        # Set up question collector
        compiled_questions = []

        if survey_detail_page['question_count'] == 0:
            return None

        # Loop thorugh questions
        for question in survey_detail_page['questions']:

            # Convert results to row that have a single response
            qdata = json_normalize(question, sep='_')
            qdata = qdata[[x for x in qdata.columns if 'answers' not in x if 'heading' not in x]]
            qdata.columns = ['question_' + x for x in qdata.columns]
            qdata.index = [1] * qdata.shape[0]

            # Conert heddings to wide format to merge onto single response
            headings = json_normalize(question['headings'], sep='_')
            headings = headings.T
            headings.columns = ['heading_' + str(x) for x in headings.columns]
            headings.index = [1]

            # Join headings
            qdata = qdata.merge(headings, left_index=True, right_index=True)

            if 'answers' in question.keys():
                for key in question['answers'].keys():
                    temp = self._normalize_answers(key, question)
                    qdata = qdata.merge(temp, left_index=True, right_index=True)

            compiled_questions.append(qdata)

        return compiled_questions

    def _normalize_answers(self, key, metadata_df):
        """ Helper function to get long results from a question object """
        results = json_normalize(metadata_df['answers'][key], sep='_')
        results.columns = [key + '_' + x for x in results.columns]
        results.index = [1] * results.shape[0]
        return results

    def get_survey_responses(self, survey_id):
        """ Make calls to loop through all survey responses """

        url = self.host + 'surveys/%s/responses/bulk/?per_page=100' % (survey_id)
        response = self.client.get(url).json()
        results = [self._parse_bulk_responses(response)]

        while 'next' in response['links']:
            response = self.client.get(response['links']['next']).json()
            results.append(self._parse_bulk_responses(response))

        results = pd.concat(results)
        results = results.rename(columns={'id': 'respondent_id'})
        results = results.drop(labels='page_path', axis=1)

        for col in results.columns:
            if "_id" in col:
                results[col] = results[col].astype(str)

        return results

    def _parse_bulk_responses(self, response):
        """ Helper function to get responses from response object """
        compiled_responses = []

        for respondent in response['data']:
            metadata = json_normalize(respondent, sep='_')
            metadata = metadata.drop(labels='pages', axis=1)
            metadata.index = [1] * metadata.shape[0]

            all_responses = []

            for page in respondent['pages']:
                if len(page['questions']) > 0:
                    responses = json_normalize(page['questions'], 'answers', 'id', sep='_')
                    responses = responses.rename(columns={'id': 'question_id'})
                    all_responses.append(responses)

            all_responses = pd.concat(all_responses)
            all_responses.index = [1] * all_responses.shape[0]
            all_responses = all_responses.merge(metadata, left_index=True, right_index=True)

            compiled_responses.append(all_responses)

        return pd.concat(compiled_responses)
