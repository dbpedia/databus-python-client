"""
Layer 2 Conversion Testing Script
Tests every conversion combination systematically.
Base fixture files live under tests/resources/ (base.ttl, base.nq, base.csv).
Outputs go to test_outputs/ folder.
Test file for testing with real datasets from databus.
"""

# TODO: This script is a temporary manual integration test artifact.
# It must be removed or rewritten as proper pytest integration tests
# before the final PR. Do not commit this file to the upstream repo.

import os
from databusclient.api.convert import (
    convert_rdf_triple_format,
    convert_rdf_quad_format,
    convert_tabular_format,
)

# ---------------------------------------------------------------------------
# Setup output folders
# ---------------------------------------------------------------------------

folders = [
    "test_outputs/triples/T1_turtle_to_ntriples",
    "test_outputs/triples/T2_turtle_to_rdfxml",
    "test_outputs/triples/T3_ntriples_to_turtle",
    "test_outputs/triples/T4_ntriples_to_rdfxml",
    "test_outputs/triples/T5_rdfxml_to_turtle",
    "test_outputs/triples/T6_rdfxml_to_ntriples",
    "test_outputs/quads/Q1_nquads_to_trig",
    "test_outputs/quads/Q2_nquads_to_trix",
    "test_outputs/quads/Q3_nquads_to_jsonld",
    "test_outputs/quads/Q4_trig_to_nquads",
    "test_outputs/quads/Q5_trig_to_trix",
    "test_outputs/quads/Q6_trig_to_jsonld",
    "test_outputs/quads/Q7_trix_to_nquads",
    "test_outputs/quads/Q8_trix_to_trig",
    "test_outputs/quads/Q9_trix_to_jsonld",
    "test_outputs/quads/Q10_jsonld_to_nquads",
    "test_outputs/quads/Q11_jsonld_to_trig",
    "test_outputs/quads/Q12_jsonld_to_trix",
    "test_outputs/tabular/TAB1_csv_to_tsv",
    "test_outputs/tabular/TAB2_tsv_to_csv",
]

for folder in folders:
    os.makedirs(folder, exist_ok=True)

results = []


def run_test(test_id, description, func, input_file, output_file, *args):
    """Run one conversion test and record the result."""
    try:
        func(input_file, output_file, *args)
        size = os.path.getsize(output_file)
        results.append(f"PASS  {test_id}: {description} -> {os.path.basename(output_file)} ({size} bytes)")
        return output_file
    except Exception as e:
        results.append(f"FAIL  {test_id}: {description} -> ERROR: {e}")
        return None


# ---------------------------------------------------------------------------
# GROUP 1: RDF Triple Format Conversions
# 6 combinations: each format -> every other format
# Base file: test_outputs/base/base.ttl (real DBpedia Turtle data)
# Chain: turtle -> ntriples -> rdfxml -> back to turtle
# ---------------------------------------------------------------------------

print("\n=== GROUP 1: RDF TRIPLE FORMAT CONVERSIONS ===\n")

BASE_TTL = "tests/resources/base.ttl"

# T1: turtle -> ntriples (from base turtle file)
t1_out = "test_outputs/triples/T1_turtle_to_ntriples/output.nt"
run_test(
    "T1", "turtle -> ntriples",
    convert_rdf_triple_format,
    BASE_TTL, t1_out, "turtle", "ntriples"
)

# T2: turtle -> rdf-xml (from base turtle file)
t2_out = "test_outputs/triples/T2_turtle_to_rdfxml/output.rdf"
run_test(
    "T2", "turtle -> rdf-xml",
    convert_rdf_triple_format,
    BASE_TTL, t2_out, "turtle", "rdf-xml"
)

# T3: ntriples -> turtle (uses T1 output)
t3_out = "test_outputs/triples/T3_ntriples_to_turtle/output.ttl"
if t1_out and os.path.exists(t1_out):
    run_test(
        "T3", "ntriples -> turtle",
        convert_rdf_triple_format,
        t1_out, t3_out, "ntriples", "turtle"
    )
else:
    results.append("SKIP  T3: ntriples -> turtle (T1 output not available)")

# T4: ntriples -> rdf-xml (uses T1 output)
t4_out = "test_outputs/triples/T4_ntriples_to_rdfxml/output.rdf"
if t1_out and os.path.exists(t1_out):
    run_test(
        "T4", "ntriples -> rdf-xml",
        convert_rdf_triple_format,
        t1_out, t4_out, "ntriples", "rdf-xml"
    )
else:
    results.append("SKIP  T4: ntriples -> rdf-xml (T1 output not available)")

# T5: rdf-xml -> turtle (uses T2 output)
t5_out = "test_outputs/triples/T5_rdfxml_to_turtle/output.ttl"
if t2_out and os.path.exists(t2_out):
    run_test(
        "T5", "rdf-xml -> turtle",
        convert_rdf_triple_format,
        t2_out, t5_out, "rdf-xml", "turtle"
    )
else:
    results.append("SKIP  T5: rdf-xml -> turtle (T2 output not available)")

# T6: rdf-xml -> ntriples (uses T2 output)
t6_out = "test_outputs/triples/T6_rdfxml_to_ntriples/output.nt"
if t2_out and os.path.exists(t2_out):
    run_test(
        "T6", "rdf-xml -> ntriples",
        convert_rdf_triple_format,
        t2_out, t6_out, "rdf-xml", "ntriples"
    )
else:
    results.append("SKIP  T6: rdf-xml -> ntriples (T2 output not available)")


# ---------------------------------------------------------------------------
# GROUP 2: RDF Quad Format Conversions
# 12 combinations: each of 4 formats -> every other format (4*3=12)
# Base file: test_outputs/base/base.nq
# Chain: nquads -> trig -> trix -> jsonld -> back to nquads
# ---------------------------------------------------------------------------

print("\n=== GROUP 2: RDF QUAD FORMAT CONVERSIONS ===\n")

BASE_NQ = "tests/resources/base.nq"

# Q1: nquads -> trig
q1_out = "test_outputs/quads/Q1_nquads_to_trig/output.trig"
run_test(
    "Q1", "nquads -> trig",
    convert_rdf_quad_format,
    BASE_NQ, q1_out, "nquads", "trig"
)

# Q2: nquads -> trix
q2_out = "test_outputs/quads/Q2_nquads_to_trix/output.trix"
run_test(
    "Q2", "nquads -> trix",
    convert_rdf_quad_format,
    BASE_NQ, q2_out, "nquads", "trix"
)

# Q3: nquads -> json-ld
q3_out = "test_outputs/quads/Q3_nquads_to_jsonld/output.jsonld"
run_test(
    "Q3", "nquads -> json-ld",
    convert_rdf_quad_format,
    BASE_NQ, q3_out, "nquads", "json-ld"
)

# Q4: trig -> nquads (uses Q1 output)
q4_out = "test_outputs/quads/Q4_trig_to_nquads/output.nq"
if q1_out and os.path.exists(q1_out):
    run_test(
        "Q4", "trig -> nquads",
        convert_rdf_quad_format,
        q1_out, q4_out, "trig", "nquads"
    )
else:
    results.append("SKIP  Q4: trig -> nquads (Q1 output not available)")

# Q5: trig -> trix (uses Q1 output)
q5_out = "test_outputs/quads/Q5_trig_to_trix/output.trix"
if q1_out and os.path.exists(q1_out):
    run_test(
        "Q5", "trig -> trix",
        convert_rdf_quad_format,
        q1_out, q5_out, "trig", "trix"
    )
else:
    results.append("SKIP  Q5: trig -> trix (Q1 output not available)")

# Q6: trig -> json-ld (uses Q1 output)
q6_out = "test_outputs/quads/Q6_trig_to_jsonld/output.jsonld"
if q1_out and os.path.exists(q1_out):
    run_test(
        "Q6", "trig -> json-ld",
        convert_rdf_quad_format,
        q1_out, q6_out, "trig", "json-ld"
    )
else:
    results.append("SKIP  Q6: trig -> json-ld (Q1 output not available)")

# Q7: trix -> nquads (uses Q2 output)
q7_out = "test_outputs/quads/Q7_trix_to_nquads/output.nq"
if q2_out and os.path.exists(q2_out):
    run_test(
        "Q7", "trix -> nquads",
        convert_rdf_quad_format,
        q2_out, q7_out, "trix", "nquads"
    )
else:
    results.append("SKIP  Q7: trix -> nquads (Q2 output not available)")

# Q8: trix -> trig (uses Q2 output)
q8_out = "test_outputs/quads/Q8_trix_to_trig/output.trig"
if q2_out and os.path.exists(q2_out):
    run_test(
        "Q8", "trix -> trig",
        convert_rdf_quad_format,
        q2_out, q8_out, "trix", "trig"
    )
else:
    results.append("SKIP  Q8: trix -> trig (Q2 output not available)")

# Q9: trix -> json-ld (uses Q2 output)
q9_out = "test_outputs/quads/Q9_trix_to_jsonld/output.jsonld"
if q2_out and os.path.exists(q2_out):
    run_test(
        "Q9", "trix -> json-ld",
        convert_rdf_quad_format,
        q2_out, q9_out, "trix", "json-ld"
    )
else:
    results.append("SKIP  Q9: trix -> json-ld (Q2 output not available)")

# Q10: json-ld -> nquads (uses Q3 output)
q10_out = "test_outputs/quads/Q10_jsonld_to_nquads/output.nq"
if q3_out and os.path.exists(q3_out):
    run_test(
        "Q10", "json-ld -> nquads",
        convert_rdf_quad_format,
        q3_out, q10_out, "json-ld", "nquads"
    )
else:
    results.append("SKIP  Q10: json-ld -> nquads (Q3 output not available)")

# Q11: json-ld -> trig (uses Q3 output)
q11_out = "test_outputs/quads/Q11_jsonld_to_trig/output.trig"
if q3_out and os.path.exists(q3_out):
    run_test(
        "Q11", "json-ld -> trig",
        convert_rdf_quad_format,
        q3_out, q11_out, "json-ld", "trig"
    )
else:
    results.append("SKIP  Q11: json-ld -> trig (Q3 output not available)")

# Q12: json-ld -> trix (uses Q3 output)
q12_out = "test_outputs/quads/Q12_jsonld_to_trix/output.trix"
if q3_out and os.path.exists(q3_out):
    run_test(
        "Q12", "json-ld -> trix",
        convert_rdf_quad_format,
        q3_out, q12_out, "json-ld", "trix"
    )
else:
    results.append("SKIP  Q12: json-ld -> trix (Q3 output not available)")


# ---------------------------------------------------------------------------
# GROUP 3: Tabular Format Conversions
# 2 combinations: csv->tsv and tsv->csv
# ---------------------------------------------------------------------------

print("\n=== GROUP 3: TABULAR FORMAT CONVERSIONS ===\n")

BASE_CSV = "tests/resources/base.csv"
BASE_TSV = "tests/resources/base.tsv"

# TAB1: csv -> tsv
tab1_out = "test_outputs/tabular/TAB1_csv_to_tsv/output.tsv"
run_test(
    "TAB1", "csv -> tsv",
    convert_tabular_format,
    BASE_CSV, tab1_out, "csv", "tsv"
)

# TAB2: tsv -> csv (uses TAB1 output)
tab2_out = "test_outputs/tabular/TAB2_tsv_to_csv/output.csv"
if tab1_out and os.path.exists(tab1_out):
    run_test(
        "TAB2", "tsv -> csv",
        convert_tabular_format,
        tab1_out, tab2_out, "tsv", "csv"
    )
else:
    results.append("SKIP  TAB2: tsv -> csv (TAB1 output not available)")


# ---------------------------------------------------------------------------
# GROUP 4: CLI End-to-End Tests (compressed real Databus file)
# These test the full pipeline including download.py wiring
# ---------------------------------------------------------------------------

print("\n=== GROUP 4: CLI END-TO-END (run these manually) ===\n")
cli_tests = [
    "CLI1: turtle->ntriples from compressed Databus file",
    "  poetry run databusclient download \"https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=cy.ttl.bz2\" --format ntriples --localdir ./test_outputs/cli/CLI1",
    "",
    "CLI2: turtle->rdf-xml from compressed Databus file",
    "  poetry run databusclient download \"https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=cy.ttl.bz2\" --format rdf-xml --localdir ./test_outputs/cli/CLI2",
    "",
    "CLI3: turtle->ntriples + compression bz2->gz",
    "  poetry run databusclient download \"https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=cy.ttl.bz2\" --format ntriples --compression gz --localdir ./test_outputs/cli/CLI3",
    "",
    "CLI4: turtle->ntriples + compression bz2->xz",
    "  poetry run databusclient download \"https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=cy.ttl.bz2\" --format ntriples --compression xz --localdir ./test_outputs/cli/CLI4",
    "",
    "CLI5: unsupported cross-class error (expect ValueError)",
    "  poetry run databusclient download \"https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=cy.ttl.bz2\" --format nquads --localdir ./test_outputs/cli/CLI5",
]
for line in cli_tests:
    print(line)


# ---------------------------------------------------------------------------
# Print summary
# ---------------------------------------------------------------------------

print("\n" + "="*60)
print("LAYER 2 CONVERSION TEST SUMMARY")
print("="*60)
for result in results:
    print(result)

passed = sum(1 for r in results if r.startswith("PASS"))
failed = sum(1 for r in results if r.startswith("FAIL"))
skipped = sum(1 for r in results if r.startswith("SKIP"))

print(f"\nTotal: {passed} passed, {failed} failed, {skipped} skipped")
print("="*60)