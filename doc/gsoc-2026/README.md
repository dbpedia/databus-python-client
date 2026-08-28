# GSoC 2026

Hi, I'm Dhanashree Petare ([GitHub](https://github.com/DhanashreePetare)), and I contributed to this project as part of Google Summer of Code 2026, under the DBpedia organization.

My project extended the Databus Python Client with reproducible, workflow-aware data operations, delivered across five milestones:

1. **Format and Mapping Conversion Layer** - RDF triple, RDF quad, and tabular format conversion during download, bringing the Python client to feature parity with the Java client. [CLI usage](../cli-usage.md).
2. **Structured Run Manifest System** - JSON-LD manifests recording operation parameters, file metadata, checksums, and execution results for every `download`, `deploy`, and `delete` run. [CLI usage](../cli-usage.md).
3. **Manifest Replay and Summary** - re-executing a past operation from its saved manifest, and printing a readable console summary of any manifest. [CLI usage](../cli-usage.md).
4. **Declarative Workflow Engine** - YAML-defined pipelines chaining `download`/`deploy`/`delete` steps, with step-to-step output chaining and per-step error handling (`fail`/`continue`/`retry`). [CLI usage](../cli-usage.md).
5. **Workflow-Manifest Integration and Example Workflows** - a unified manifest covering an entire workflow run, an automatic console summary, and eight example workflows. [Workflow examples](../examples/).

My project proposal is available [here](proposal_DhanashreePetare.pdf).

Thank you.