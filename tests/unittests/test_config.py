import unittest
from tap_snapchat_ads import maybe_parse_org_account_ids


class TestMaybeParseOrgAccountIds(unittest.TestCase):
    config_with_list = {'org_account_ids':
                        [{'organisation_id': 'id1', 'ad_accounts': []},
                         {'organisation_id': 'id2', 'ad_accounts': ['ad_id1', 'ad_id2']}],
                        'start_date': '2024-02-24T00:00:00Z',}

    config_with_string = {'org_account_ids':
                          "[{\"organisation_id\":\"id1\",\"ad_accounts\":[]},{\"organisation_id\":\"id2\",\"ad_accounts\":[\"ad_id1\",\"ad_id2\"]}]",
                          'start_date': '2024-02-24T00:00:00Z',}

    config_with_bad_type = {'org_account_ids':
                            12345,
                            'start_date': '2024-02-24T00:00:00Z',}

    def test_with_list(self):
        """Test that config with org_account_ids of type list remains unchanged"""
        self.assertIsInstance(self.config_with_list["org_account_ids"], list)
        maybe_parse_org_account_ids(self.config_with_list)
        self.assertIsInstance(self.config_with_list["org_account_ids"], list)
        self.assertEqual(self.config_with_list["start_date"], '2024-02-24T00:00:00Z')

    def test_with_string(self):
        """Test that config with org_account_ids of type string is converted to type list"""
        self.assertIsInstance(self.config_with_string["org_account_ids"], str)
        maybe_parse_org_account_ids(self.config_with_string)
        self.assertIsInstance(self.config_with_string["org_account_ids"], list)
        self.assertEqual(self.config_with_string["start_date"], '2024-02-24T00:00:00Z')
        self.assertEqual(self.config_with_string, self.config_with_list)

    def test_with_bad_type(self):
        """Test that config with org_account_ids with unexpected type is unchanged by the function"""
        self.assertIsInstance(self.config_with_bad_type["org_account_ids"], int)
        maybe_parse_org_account_ids(self.config_with_bad_type)
        self.assertIsInstance(self.config_with_bad_type["org_account_ids"], int)
        self.assertEqual(self.config_with_bad_type["start_date"], '2024-02-24T00:00:00Z')
