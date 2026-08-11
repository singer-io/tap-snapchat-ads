"""
Coverage tests for tap_snapchat_ads/discover.py and tap_snapchat_ads/schema.py.
Covers the full discover() and get_schemas() functions.
"""
import io
import json
import unittest

from singer.catalog import Catalog

from tap_snapchat_ads.discover import discover
from tap_snapchat_ads.schema import get_schemas, get_abs_path


class TestGetAbsPath(unittest.TestCase):
    def test_returns_absolute_path(self):
        # line 13 of schema.py
        result = get_abs_path('schemas/organizations.json')
        self.assertIn('schemas/organizations.json', result)
        self.assertTrue(result.startswith('/'))


class TestGetSchemas(unittest.TestCase):
    def test_returns_all_streams(self):
        # lines 16-50 of schema.py
        schemas, field_metadata = get_schemas()
        self.assertIsInstance(schemas, dict)
        self.assertGreater(len(schemas), 0)
        self.assertIn('organizations', schemas)
        self.assertIn('ad_accounts', schemas)

    def test_schema_has_properties(self):
        schemas, _ = get_schemas()
        org_schema = schemas['organizations']
        self.assertIn('properties', org_schema)

    def test_field_metadata_matches_streams(self):
        schemas, field_metadata = get_schemas()
        self.assertEqual(set(schemas.keys()), set(field_metadata.keys()))

    def test_replication_keys_marked_automatic(self):
        _, field_metadata = get_schemas()
        orgs_meta = field_metadata['organizations']
        # breadcrumbs are tuples (not lists) in singer metadata
        updated_at_meta = next(
            (m for m in orgs_meta if m.get('breadcrumb') == ('properties', 'updated_at')),
            None,
        )
        self.assertIsNotNone(updated_at_meta)
        self.assertEqual(updated_at_meta['metadata'].get('inclusion'), 'automatic')

    def test_parent_tap_stream_id_set_for_child_streams(self):
        _, field_metadata = get_schemas()
        ad_accounts_meta = field_metadata['ad_accounts']
        # root breadcrumb is empty tuple ()
        root_meta = next(
            (m for m in ad_accounts_meta if m.get('breadcrumb') == ()),
            None,
        )
        self.assertIsNotNone(root_meta)
        self.assertEqual(root_meta['metadata'].get('parent-tap-stream-id'), 'organizations')


class TestDiscover(unittest.TestCase):
    def test_returns_catalog_with_streams(self):
        # lines 3-26 of discover.py
        catalog = discover()
        self.assertIsInstance(catalog, Catalog)
        self.assertGreater(len(catalog.streams), 0)

    def test_catalog_has_key_properties(self):
        catalog = discover()
        org_entry = next((s for s in catalog.streams if s.tap_stream_id == 'organizations'), None)
        self.assertIsNotNone(org_entry)
        self.assertIn('id', org_entry.key_properties)

    def test_catalog_serialisable(self):
        catalog = discover()
        captured = io.StringIO()
        json.dump(catalog.to_dict(), captured, indent=2)
        data = json.loads(captured.getvalue())
        self.assertIn('streams', data)
