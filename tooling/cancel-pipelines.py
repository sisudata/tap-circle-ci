import os
import requests
import sys


HEADERS = {
    'Accept': 'application/json',
    'Circle-Token': os.environ['CIRCLECI_TOKEN']
}


def get(url):
    return requests.get(
        url,
        headers=HEADERS
    )


def post(url):
    return requests.post(
        url,
        headers=HEADERS
    )


def get_all_pipelines(project_slug, last_page, start, end):
    formatted_page = last_page
    last_updated_at = None
    while True:
        response = get(
            f'https://circleci.com/api/v2/project/{project_slug}/pipeline{formatted_page}'
        ).json()

        for pipeline in response.get('items', []):
            if start > pipeline['updated_at']:
                return None
            if end > pipeline['updated_at']:
                yield pipeline
            last_updated_at = pipeline['updated_at']

        if response.get('next_page_token'):
            formatted_page = f"?page-token={response.get('next_page_token')}"
            print(f'get_all_pipelines: Fetching next page... {last_updated_at}')
        else:
            return None


def get_all_workflows(pipeline):
    """
    https://circleci.com/docs/api/v2/#operation/listWorkflowsByPipelineId
    """
    formatted_page = ''
    while True:
        response = get(
            f'https://circleci.com/api/v2/pipeline/{pipeline["id"]}/workflow{formatted_page}'
        ).json()

        for workflow in response.get('items', []):
            yield workflow

        if response.get('next_page_token'):
            formatted_page = f"?page-token={response.get('next_page_token')}"
            print(f'get_all_workflows: Fetching next page... {formatted_page}')
        else:
            return None


def pipeline_is_completed(workflows):
    running_workflows = [True for w in workflows if not w.get('stopped_at')]
    return not running_workflows


def cancel_pipeline(pipeline, workflows):
    print(
        f'  Cancelling pipeline: https://app.circleci.com/pipelines/{pipeline["project_slug"]}/{pipeline["number"]}')
    for workflow in workflows:
        if not workflow.get('stopped_at'):
            print(
                f'    Cancelling workflow: https://app.circleci.com/pipelines/{pipeline["project_slug"]}/{pipeline["number"]}/workflows/{workflow["id"]}')
            response = post(
                f'https://circleci.com/api/v2/workflow/{workflow["id"]}/cancel'
            ).json()

            if not response['message'] == 'Accepted.':
                raise Exception(f'{response}')

    print('  Cancelled.')


def main(args):
    project_slug = args[1]
    start = args[2]
    end = args[3]
    for pipeline in get_all_pipelines(project_slug, '', start, end):
        workflows = list(get_all_workflows(pipeline))
        if not pipeline_is_completed(workflows):
            cancel_pipeline(pipeline, workflows)


main(sys.argv)
