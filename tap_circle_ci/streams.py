import datetime
from typing import List, Optional

import singer
import singer.metadata as metadata_lib
import singer.bookmarks as bookmarks
import singer.metrics as metrics
from tap_circle_ci.client import get_all_items

LOGGER = singer.get_logger()

# We leave a week offset for currently running pipelines
TIME_BUFFER_FOR_RUNNING_PIPELINES = datetime.timedelta(days=7)


def get_bookmark(state: dict, project: str, stream_name: str, bookmark_key: str) -> Optional[str]:
    """
    Retrieve a bookmark from the bookmarks (basically marks when you last synced records for this key)
    """
    stream_bookmark = bookmarks.get_bookmark(state, project, stream_name)
    if stream_bookmark is not None:
        return stream_bookmark.get(bookmark_key)
    return None


def pipeline_is_completed(workflows):
    running_workflows = [True for w in workflows if not w['stopped_at']]
    return not running_workflows


def get_all_pipelines(schemas: dict, project: str, state: dict, metadata: dict, options: dict = {}) -> dict:
    """
    https://circleci.com/docs/api/v2/#operation/listPipelines
    """
    bookmark = get_bookmark(state, project, "pipelines", "since")
    if bookmark:
        bookmark_time = singer.utils.strptime_to_utc(bookmark)
    else:
        bookmark_time = None

    pipeline_url = f'https://circleci.com/api/v2/project/{project}/pipeline'
    pipeline_counter = metrics.record_counter('pipelines')
    workflow_counter = metrics.record_counter(
        'workflows') if schemas.get('workflows') else None
    job_counter = metrics.record_counter(
        'jobs') if schemas.get('jobs') else None
    step_counter = metrics.record_counter(
        'steps') if schemas.get('steps') else None
    extraction_time = singer.utils.now()

    updated_at = None

    # Pipelines are ordered updated_at desc
    for pipeline in get_all_items('pipelines', pipeline_url):
        # grab all workflows for this pipeline
        workflow_extraction_time = singer.utils.now()
        workflows = get_all_workflows_for_pipeline(
            schemas,
            pipeline.get("id"),
            metadata
        )

        pipeline_updated_at = singer.utils.strptime_to_utc(pipeline['updated_at'])
        # We set the updated_at to be the first pipeline[updated_at] to serve as a temporary bookmark
        if updated_at is None:
            updated_at = pipeline_updated_at

        # We avoid extracting if the updated time of the pipeline is less than our bookmark_time
        if bookmark_time is not None and pipeline_updated_at < bookmark_time:
            continue

        # We terminate extracting once we come across currently running pipelines
        if not pipeline_is_completed(workflows):
            continue

        # Transform and write record
        with singer.Transformer() as transformer:
            record = transformer.transform(
                pipeline,
                schemas['pipelines'].to_dict(),
                metadata=metadata_lib.to_map(metadata['pipelines'])
            )
        singer.write_record('pipelines', record,
                            time_extracted=extraction_time)
        updated_at = min(updated_at, pipeline_updated_at)
        pipeline_counter.increment()

        emit_all_workflows_for_pipeline(
            schemas,
            pipeline.get("id"),
            project,
            workflow_extraction_time,
            workflows,
            workflow_counter,
            job_counter,
            step_counter
        )

    # Update bookmarks after extraction
    singer.write_bookmark(state, project, 'pipelines', {
                          'since': singer.utils.strftime(updated_at)})

    return state


def get_all_workflows_for_pipeline(
    schemas: dict,
    pipeline_id: str,
    metadata: dict
) -> dict:
    """
    https://circleci.com/docs/api/v2/#operation/listWorkflowsByPipelineId
    """
    workflow_url = f"https://circleci.com/api/v2/pipeline/{pipeline_id}/workflow"

    with singer.Transformer() as transformer:
        for workflow in get_all_items('workflows', workflow_url):
            yield transformer.transform(
                workflow,
                schemas['workflows'].to_dict(),
                metadata=metadata_lib.to_map(metadata['workflows'])
            )


def emit_all_workflows_for_pipeline(
    schemas: dict,
    pipeline_id: str,
    project: str,
    extraction_time: str,
    workflows: list,
    metadata: dict,
    workflow_counter: Optional[metrics.Counter] = None,
    job_counter: Optional[metrics.Counter] = None,
    step_counter: Optional[metrics.Counter] = None
):
    if workflow_counter is None:
        workflow_counter = metrics.record_counter('workflows')

    for workflow in workflows:
        singer.write_record('workflows', workflow,
                            time_extracted=extraction_time)
        workflow_counter.increment()

        # If jobs are selected, grab all the jobs for this workflow
        if schemas.get('jobs'):
            get_all_jobs_for_workflow(
                schemas,
                pipeline_id,
                workflow,
                project,
                metadata,
                job_counter,
                step_counter
            )


def get_all_jobs_for_workflow(
    schemas: dict,
    pipeline_id: str,
    workflow: dict,
    metadata: dict,
    job_counter: Optional[metrics.Counter] = None,
    step_counter: Optional[metrics.Counter] = None
) -> dict:
    """
    https://circleci.com/docs/api/v2/#operation/listWorkflowJobs
    """

    workflow_id = workflow.get("id")

    if job_counter is None:
        job_counter = metrics.record_counter('jobs')

    job_url = f"https://circleci.com/api/v2/workflow/{workflow_id}/job"
    extraction_time = singer.utils.now()
    for job in get_all_items('jobs', job_url):

        # add in workflow_id and pipeline_id
        job.update({
            '_pipeline_id': pipeline_id,
            '_workflow_id': workflow_id,
            'pipeline_id': pipeline_id,
            'workflow_id': workflow_id,
        })

        # Transform and write
        with singer.Transformer() as transformer:
            record = transformer.transform(
                job,
                schemas['jobs'].to_dict(),
                metadata=metadata_lib.to_map(metadata['jobs'])
            )
        singer.write_record('jobs', record, time_extracted=extraction_time)
        job_counter.increment()

        # If steps are selected, grab all the steps for this job
        if schemas.get('steps') and job.get("type") == "build":
            get_all_steps_for_job(
                schemas,
                pipeline_id,
                workflow,
                job,
                metadata,
                step_counter
            )


def get_all_steps_for_job(
    schemas: dict,
    pipeline_id: str,
    workflow: dict,
    job: dict,
    metadata: dict,
    step_counter: Optional[metrics.Counter] = None
) -> dict:
    """
    https://circleci.com/docs/api/#single-job
    """

    if step_counter is None:
        step_counter = metrics.record_counter('jobs')

    workflow_id = workflow.get("id")
    slug = workflow.get("project_slug")
    build_num = job.get("job_number")

    build_url = f"https://circleci.com/api/v1.1/project/{slug}/{build_num}"
    extraction_time = singer.utils.now()
    for build in get_all_items('steps', build_url):
        for idx, step in enumerate(build["steps"]):

            # add in workflow_id, pipeline_id and job_id
            step.update({
                '_pipeline_id': pipeline_id,
                '_workflow_id': workflow_id,
                '_job_id': workflow_id,
                'pipeline_id': pipeline_id,
                'workflow_id': workflow_id,
                'job_id': workflow_id,
                '_index': idx
            })

            # Transform and write
            with singer.Transformer() as transformer:
                record = transformer.transform(
                    step,
                    schemas['steps'].to_dict(),
                    metadata=metadata_lib.to_map(metadata['steps'])
                )
            singer.write_record(
                'steps', record, time_extracted=extraction_time)
            step_counter.increment()


# The following is boiler-plate to help the main function map stream ids to the actual function to sync the stream
# Don't contain sub streams bc those are synced by parents
TOP_LEVEL_STREAM_ID_TO_FUNCTION = {
    'pipelines': get_all_pipelines
}

STREAM_ID_TO_SUB_STREAM_IDS = {
    'pipelines': ['workflows'],
    'workflows': ['jobs'],
    'jobs': ['steps']
}


def validate_stream_dependencies(selected_stream_ids: List[str]) -> None:
    """Validates that substreams parents are also selected"""
    sub_stream_ids_to_parents = {}
    for k, v in STREAM_ID_TO_SUB_STREAM_IDS.items():
        for child in v:
            if child in sub_stream_ids_to_parents:
                # This indicates an error in the code
                raise ValueError("A sub stream cannot have multiple parents!!")
            sub_stream_ids_to_parents[child] = k

    for id in selected_stream_ids:
        if id not in TOP_LEVEL_STREAM_ID_TO_FUNCTION:
            if sub_stream_ids_to_parents[id] not in selected_stream_ids:
                raise ValueError(
                    f"Stream {id} is dependent on {sub_stream_ids_to_parents[id]}! Please select "
                    f"{sub_stream_ids_to_parents[id]} as well to load records for {id}"
                )
