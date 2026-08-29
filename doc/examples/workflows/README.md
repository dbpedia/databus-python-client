# Example Workflows

Eight example workflow pipelines, each runnable directly (though the deploy/delete steps use paths under a specific Databus account -- swap in your own account/version paths before running them yourself). All use real, existing Databus data as their download source.

```bash
export DATABUS_API_KEY=your-key-here
databusclient workflow run download-deploy.yml
```

## Basic examples

- **`download-deploy.yml`** - downloads a real Databus dataset, then redeploys it exactly as downloaded (classic deploy mode, using `${steps.name.output_urls}` - the actual, redirect-resolved source URL, not the local file).
- **`download-delete.yml`** - downloads a real Databus dataset, then deletes that same version, demonstrating a realistic archive-then-delete workflow.
- **`full-pipeline.yml`** - chains all three commands together: download a real Databus dataset, deploy it, then delete that same deployed version.

## The five proposal use cases (Milestone 5)

- **`reproducible-research-download.yml`** - downloads a dataset with checksum validation and a saved manifest, so the exact same download can be verified or reproduced later.
- **`nightly-publishing-pipeline.yml`** - download, deploy, then clean up an old version, meant to run unattended (e.g. via cron), with a unified manifest for the whole run.
- **`batch-deployment-with-retry.yml`** - deploys multiple versions in one run, with `on_error: retry` configured on each deploy step to handle transient failures automatically.
- **`ci-cd-integration.yml`** - a workflow with no interactive prompts anywhere, safe to call as a step in a CI/CD pipeline such as GitHub Actions.
- **`failure-debugging.yml`** - intentionally fails a deploy step (invalid API key) to demonstrate what a failed workflow's console output and manifest look like.

## Manifests

Workflows can write a unified manifest covering every step in two ways: pass `--manifest path.jsonld` on the command line, or set a top-level `manifest:` key inside the YAML file itself (the command-line flag takes priority if both are given). Several of the examples above use the YAML key. Every manifest file entry that came from a workflow step is tagged with `dbus:stepName`, so a multi-step run stays traceable to which step produced or failed on which file.

See the [CLI usage documentation](../../cli-usage.md#cli-workflow) for the full YAML format, step chaining, error handling, and WebDAV deploy mode documentation.
