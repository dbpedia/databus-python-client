"""
SPARQL Queries for Databus Python Client

This module contains SPARQL queries used for interacting with the DBpedia Databus.
"""

# Query to fetch ontologies with proper content variant aggregation
# Uses GROUP_CONCAT to handle multiple content variants per distribution
ONTOLOGIES_QUERY = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX databus: <https://databus.dbpedia.org/>
PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
PREFIX dataid-cv: <http://dataid.dbpedia.org/ns/cv#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT 
  ?group ?art ?version ?title ?publisher ?comment ?description 
  ?license ?file ?extension ?type ?bytes ?shasum 
  (GROUP_CONCAT(DISTINCT ?variantStr; separator=", ") AS ?contentVariants) 
WHERE { 
  ?dataset dataid:account databus:ontologies .
  ?dataset dataid:group ?group .
  ?dataset dataid:artifact ?art.
  ?dataset dcat:distribution ?distribution .
  ?dataset dct:license ?license .
  ?dataset dct:publisher ?publisher .
  ?dataset rdfs:comment ?comment .
  ?dataset dct:description ?description .
  ?dataset dct:title ?title .
  ?distribution dcat:downloadURL ?file .
  ?distribution dataid:formatExtension ?extension .
  ?distribution dataid-cv:type ?type .
  ?distribution dcat:byteSize ?bytes .
  ?distribution dataid:sha256sum ?shasum .
  ?dataset dct:hasVersion ?version .

  # Excludes dev versions
  FILTER (!regex(?art, "--DEV"))

  # OPTIONAL: Check for variants, but don't fail if none exist
  OPTIONAL { 
    ?distribution dataid:contentVariant ?cv . 
    BIND(STR(?cv) AS ?variantStr)
  }

} 
GROUP BY ?group ?art ?version ?title ?publisher ?comment ?description ?license ?file ?extension ?type ?bytes ?shasum 
ORDER BY ?version
"""


def parse_content_variants_string(variants_str: str) -> dict:
    """
    Parse a comma-separated content variants string from SPARQL GROUP_CONCAT result.
    
    Parameters
    ----------
    variants_str : str
        Comma-separated string of content variants, e.g., "lang=en, type=full, sorted=true"
    
    Returns
    -------
    dict
        Dictionary of key-value pairs, e.g., {"lang": "en", "type": "full", "sorted": "true"}
    """
    if not variants_str or variants_str.strip() == "":
        return {}
    
    variants = {}
    for part in variants_str.split(","):
        part = part.strip()
        if "=" in part:
            key, value = part.split("=", 1)
            variants[key.strip()] = value.strip()
        elif part:
            # Handle standalone values (no key=value format)
            variants[part] = True
    
    return variants
