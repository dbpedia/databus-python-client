# GSoC 2026

Hi, I'm Dhanashree Petare ([GitHub](https://github.com/DhanashreePetare)), and I contributed to this project as part of Google Summer of Code 2026, under the DBpedia organization.

My project extended the Databus Python Client with reproducible, workflow-aware data operations, delivered across five milestones:

1. **Format and Mapping Conversion Layer** - RDF triple, RDF quad, and tabular format conversion during download, bringing the Python client to feature parity with the Java client. [Download docs](../cli-usage.md#cli-download).
2. **Structured Run Manifest System** - JSON-LD manifests recording operation parameters, file metadata, checksums, and execution results for every `download`, `deploy`, and `delete` run. [Manifest docs](../cli-usage.md#cli-manifest).
3. **Manifest Replay and Summary** - re-executing a past operation from its saved manifest, and printing a readable console summary of any manifest. [Replay docs](../cli-usage.md#cli-manifest-replay) and [summary docs](../cli-usage.md#cli-manifest-summary).
4. **Declarative Workflow Engine** - YAML-defined pipelines chaining `download`/`deploy`/`delete` steps, with step-to-step output chaining and per-step error handling (`fail`/`continue`/`retry`). [Workflow docs](../cli-usage.md#cli-workflow).
5. **Workflow-Manifest Integration and Example Workflows** - a unified manifest covering an entire workflow run, an automatic console summary, and example workflows. [Workflow examples](../examples/workflows/README.md).

My project proposal is available [here](proposal_DhanashreePetare.pdf).

Thank you.