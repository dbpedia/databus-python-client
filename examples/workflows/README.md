# Example Workflows

Three example workflow pipelines, each runnable directly, though the deploy/delete steps use paths under a specific Databus account -- swap in your own account/version paths before running them yourself. All three use real, existing Databus data as their download source.

```bash
export DATABUS_API_KEY=your-key-here
databusclient workflow run download-deploy.yml
```

- **`download-deploy.yml`** - downloads a real Databus dataset, then redeploys it exactly as downloaded (classic deploy mode, using `${steps.name.output_urls}` - the actual, redirect-resolved source URL, not the local file).
- **`download-delete.yml`** - downloads a real Databus dataset, then deletes that same version, demonstrating a realistic archive-then-delete workflow.
- **`full-pipeline.yml`** - chains all three commands together: download a real Databus dataset, deploy it, then delete that same deployed version, demonstrating a complete download-deploy-cleanup pipeline.

All three set `api_key: ${DATABUS_API_KEY}` - set that environment variable before running, rather than writing a real key into the file.

See the main [README's Workflow section](../../README.md#cli-workflow) for the full YAML format, step chaining, error handling, and WebDAV deploy mode documentation.

