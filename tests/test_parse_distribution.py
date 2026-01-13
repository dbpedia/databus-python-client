"""Tests for parse_distribution_str function in cli.py"""

from unittest.mock import patch

import pytest

from databusclient.cli import parse_distribution_str
from databusclient.api.deploy import (
    create_dataset,
    _get_file_info_from_dict,
)


class TestParseDistributionStr:
    """Unit tests for parse_distribution_str function."""

    # -------------------------------------------------------------------------
    # URL Extraction Tests
    # -------------------------------------------------------------------------

    def test_basic_url_extraction(self):
        """Test that URL is correctly extracted from distribution string."""
        result = parse_distribution_str("http://example.com/data.json")
        assert result["url"] == "http://example.com/data.json"

    def test_url_with_modifiers(self):
        """Test URL extraction when modifiers are present."""
        result = parse_distribution_str("http://example.com/data.json|lang=en|.json")
        assert result["url"] == "http://example.com/data.json"

    # -------------------------------------------------------------------------
    # Content Variant Parsing Tests
    # -------------------------------------------------------------------------

    def test_single_variant(self):
        """Test parsing a single key=value variant."""
        result = parse_distribution_str("http://example.com/file|lang=en")
        assert result["variants"] == {"lang": "en"}

    def test_multiple_variants(self):
        """Test parsing multiple key=value variants."""
        result = parse_distribution_str("http://example.com/file|lang=en|type=full|quality=high")
        assert result["variants"] == {
            "lang": "en",
            "type": "full",
            "quality": "high",
        }

    def test_variant_with_equals_in_value(self):
        """Test variant where value contains equals sign."""
        result = parse_distribution_str("http://example.com/file|filter=a=b")
        assert result["variants"] == {"filter": "a=b"}

    def test_empty_variants(self):
        """Test that empty variants dict is returned when no variants present."""
        result = parse_distribution_str("http://example.com/file.json|.json")
        assert result["variants"] == {}

    # -------------------------------------------------------------------------
    # Format Extension Tests
    # -------------------------------------------------------------------------

    def test_json_extension(self):
        """Test .json format extension detection."""
        result = parse_distribution_str("http://example.com/file|.json")
        assert result["formatExtension"] == "json"

    def test_ttl_extension(self):
        """Test .ttl format extension detection."""
        result = parse_distribution_str("http://example.com/file|.ttl")
        assert result["formatExtension"] == "ttl"

    def test_csv_extension(self):
        """Test .csv format extension detection."""
        result = parse_distribution_str("http://example.com/file|.csv")
        assert result["formatExtension"] == "csv"

    def test_xml_extension(self):
        """Test .xml format extension detection."""
        result = parse_distribution_str("http://example.com/file|.xml")
        assert result["formatExtension"] == "xml"

    def test_no_extension(self):
        """Test that formatExtension is None when not provided."""
        result = parse_distribution_str("http://example.com/file|lang=en")
        assert result["formatExtension"] is None

    # -------------------------------------------------------------------------
    # Compression Detection Tests
    # -------------------------------------------------------------------------

    def test_gz_compression(self):
        """Test .gz compression detection."""
        result = parse_distribution_str("http://example.com/file|.gz")
        assert result["compression"] == "gz"

    def test_zip_compression(self):
        """Test .zip compression detection."""
        result = parse_distribution_str("http://example.com/file|.zip")
        assert result["compression"] == "zip"

    def test_br_compression(self):
        """Test .br (brotli) compression detection."""
        result = parse_distribution_str("http://example.com/file|.br")
        assert result["compression"] == "br"

    def test_tar_compression(self):
        """Test .tar compression detection."""
        result = parse_distribution_str("http://example.com/file|.tar")
        assert result["compression"] == "tar"

    def test_zst_compression(self):
        """Test .zst (zstandard) compression detection."""
        result = parse_distribution_str("http://example.com/file|.zst")
        assert result["compression"] == "zst"

    def test_no_compression(self):
        """Test that compression is None when not provided."""
        result = parse_distribution_str("http://example.com/file|.json")
        assert result["compression"] is None

    # -------------------------------------------------------------------------
    # Combined Modifiers Tests
    # -------------------------------------------------------------------------

    def test_full_distribution_string(self):
        """Test parsing a complete distribution string with all modifiers."""
        result = parse_distribution_str(
            "http://mysite.com/data.json|lang=fr|quality=high|.json|.gz"
        )
        assert result == {
            "url": "http://mysite.com/data.json",
            "variants": {"lang": "fr", "quality": "high"},
            "formatExtension": "json",
            "compression": "gz",
        }

    def test_order_independence(self):
        """Test that order of modifiers doesn't affect parsing."""
        result = parse_distribution_str(
            "http://example.com/file|.gz|lang=en|.json|type=full"
        )
        assert result["variants"] == {"lang": "en", "type": "full"}
        assert result["formatExtension"] == "json"
        assert result["compression"] == "gz"

    # -------------------------------------------------------------------------
    # Edge Cases
    # -------------------------------------------------------------------------

    def test_whitespace_handling(self):
        """Test that whitespace is properly stripped."""
        result = parse_distribution_str("http://example.com/file | lang = en | .json ")
        assert result["url"] == "http://example.com/file"
        assert result["variants"] == {"lang": "en"}
        assert result["formatExtension"] == "json"

    def test_standalone_tag_warning(self, capsys):
        """Test that standalone tags (without =) produce a warning."""
        result = parse_distribution_str("http://example.com/file|unknown_tag")
        captured = capsys.readouterr()
        assert "WARNING" in captured.out
        assert "unknown_tag" in captured.out
        # Standalone tags should not be added to variants
        assert "unknown_tag" not in result["variants"]

    def test_url_only(self):
        """Test parsing URL without any modifiers."""
        result = parse_distribution_str("http://example.com/data.json")
        assert result == {
            "url": "http://example.com/data.json",
            "variants": {},
            "formatExtension": None,
            "compression": None,
        }


class TestIntegrationWithDeployAPI:
    """Integration tests verifying parsed dicts work with api_deploy functions."""

    @patch("databusclient.api.deploy._load_file_stats")
    def test_get_file_info_from_dict_basic(self, mock_load_stats):
        """Test _get_file_info_from_dict with parsed distribution dict."""
        mock_load_stats.return_value = ("abc123" * 10 + "abcd", 12345)

        parsed = parse_distribution_str(
            "http://example.com/data.json|lang=en|type=full|.json|.gz"
        )
        cvs, ext, comp, sha, size = _get_file_info_from_dict(parsed)

        assert cvs == {"lang": "en", "type": "full"}
        assert ext == "json"
        assert comp == "gz"
        assert sha == "abc123" * 10 + "abcd"
        assert size == 12345

    @patch("databusclient.api.deploy._load_file_stats")
    def test_get_file_info_from_dict_defaults(self, mock_load_stats):
        """Test default values when extension/compression not specified."""
        mock_load_stats.return_value = ("sha256hash", 1000)

        parsed = parse_distribution_str("http://example.com/data|lang=en")
        cvs, ext, comp, sha, size = _get_file_info_from_dict(parsed)

        # Should use defaults
        assert ext == "file"  # default when not specified
        assert comp == "none"  # default when not specified

    @patch("databusclient.api.deploy._load_file_stats")
    def test_create_dataset_with_dict_distributions(self, mock_load_stats):
        """Test create_dataset accepts parsed dict distributions."""
        fake_sha = "a" * 64
        mock_load_stats.return_value = (fake_sha, 5000)

        parsed_dist = parse_distribution_str(
            "http://example.com/file.json|lang=en|.json"
        )

        dataset = create_dataset(
            version_id="https://databus.example.org/user/group/artifact/2024.01.01/",
            title="Test Dataset",
            abstract="Test abstract",
            description="Test description",
            license_url="https://example.org/license",
            distributions=[parsed_dist],
        )

        # Verify dataset structure
        assert "@context" in dataset
        assert "@graph" in dataset

        # Find distribution in graph
        graphs = dataset["@graph"]
        version_graph = next(
            (g for g in graphs if "@type" in g and "Version" in g.get("@type", [])),
            None,
        )
        assert version_graph is not None
        assert "distribution" in version_graph

        dist = version_graph["distribution"][0]
        assert dist["downloadURL"] == "http://example.com/file.json"
        assert dist["formatExtension"] == "json"
        assert dist["dcv:lang"] == "en"

    @patch("databusclient.api.deploy._load_file_stats")
    def test_create_dataset_multiple_distributions(self, mock_load_stats):
        """Test create_dataset with multiple distributions requires variants."""
        fake_sha = "b" * 64
        mock_load_stats.return_value = (fake_sha, 3000)

        dist1 = parse_distribution_str("http://example.com/en.json|lang=en|.json")
        dist2 = parse_distribution_str("http://example.com/de.json|lang=de|.json")

        dataset = create_dataset(
            version_id="https://databus.example.org/user/group/artifact/2024.01.01/",
            title="Test Dataset",
            abstract="Test abstract",
            description="Test description",
            license_url="https://example.org/license",
            distributions=[dist1, dist2],
        )

        # Both distributions should be present
        graphs = dataset["@graph"]
        version_graph = next(
            (g for g in graphs if "@type" in g and "Version" in g.get("@type", [])),
            None,
        )
        distributions = version_graph["distribution"]
        assert len(distributions) == 2

        # Verify different language variants
        langs = {d["dcv:lang"] for d in distributions}
        assert langs == {"en", "de"}
