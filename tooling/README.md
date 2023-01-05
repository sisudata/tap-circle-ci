# Tooling

## `cancel-pipelines.py`

CircleCI has no configurable timeout for Pipelines. Because of the inherent way this
tap works, it **does not** sync _beyond_ any running Pipeline. Use this script to
effectively enforce a timeout for your Pipelines to unblock your tap.

```
EXPORT CIRCLE_TOKEN=<Personal API Token>
python cancel-pipelines.py gh/your-org/your-repo 2022-01-01 2022-12-15
```

### Args

Use `start` and `end` to limit the scope of your cancelling.

| Name         | Type       | Description                                                                                                                                                                        |
| ------------ | ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| CIRCLE_TOKEN | `envvar`   | [Personal API Token](https://circleci.com/docs/managing-api-tokens/#creating-a-personal-api-token)                                                                                 |
| project slug | `string`   | https://circleci.com/docs/api/v2/index.html#operation/listPipelinesForProject "Example: gh/CircleCI-Public/api-preview-docs, Project slug in the form vcs-slug/org-name/repo-name" |
| start        | `datetime` | Probably your bookmark value, Example: 2022-08-08T06:50:57.723Z                                                                                                                    |
| end          | `datetime` | Any Pipeline which has been updated after this datetime won't be cancelled. Example: 2022-08-08T06:50:57.723Z                                                                      |
