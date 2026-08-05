# Example Workflows

Three example workflow pipelines, each runnable directly:

```bash
export DATABUS_API_KEY=your-key-here
databusclient workflow run download-deploy.yml
```

- **`download-deploy.yml`** — downloads a file, then redeploys it exactly as downloaded (classic deploy mode, using `${steps.name.output_urls}`).
- **`download-delete.yml`** — downloads a file, then deletes an old dataset version. Delete steps never prompt for confirmation inside a workflow.
- **`full-pipeline.yml`** — chains all three commands together: download, deploy, then delete a previous version, demonstrating a complete nightly-publishing-style pipeline.

All three set `api_key: ${DATABUS_API_KEY}` — set that environment variable before running, rather than writing a real key into the file.

See the main [README's Workflow section](../../README.md#cli-workflow) for the full YAML format, step chaining, error handling, and WebDAV deploy mode documentation.