"""Layer 3 prototype mapping handlers."""

import json
import os

from databusclient.filehandling.format import TSDHandler, TripleHandler


def convert_rdf_to_csv(
    input_file: str,
    output_file: str,
    input_format: str,
) -> None:
    """Map RDF triples to a wide CSV table (Layer 3 prototype).

    Each unique subject becomes a row. Each unique predicate becomes a column.
    Multi-valued predicates are pipe-separated.
    A companion .meta.json file is generated to preserve RDF datatype and
    language tag information for lossless round trips.

    NOTE: This is a Layer 3 prototype. It is not yet tested and will be
    properly implemented in the Layer 3 issue.

    Args:
        input_file: Path to input RDF triples file.
        output_file: Path to write output CSV file.
        input_format: Source triple format name (must be in RDF_TRIPLE_FORMATS).
    """
    handler = TripleHandler()
    g = handler.read(input_file, input_format)

    predicates = sorted(set(str(p) for s, p, o in g))

    subjects: dict = {}
    column_metadata: dict = {}

    for s, p, o in g:
        subj = str(s)
        pred = str(p)

        if hasattr(o, "datatype") and o.datatype:
            column_metadata[pred] = {"datatype": str(o.datatype)}
        elif hasattr(o, "language") and o.language:
            column_metadata[pred] = {"language": str(o.language)}

        if subj not in subjects:
            subjects[subj] = {}
        if pred not in subjects[subj]:
            subjects[subj][pred] = []
        subjects[subj][pred].append(str(o))

    tsd_handler = TSDHandler()
    rows = [["resource"] + predicates]
    for subj, pred_map in subjects.items():
        row = [subj]
        for pred in predicates:
            values = pred_map.get(pred, [])
            row.append("|".join(values))
        rows.append(row)

    tsd_handler.write(rows, output_file, "csv")

    companion_file = output_file + ".meta.json"
    with open(companion_file, "w", encoding="utf-8") as f:
        json.dump({"columns": column_metadata}, f, indent=2)

    print(f"Converted RDF -> CSV: {os.path.basename(output_file)}")
    print(f"Companion metadata: {os.path.basename(companion_file)}")
