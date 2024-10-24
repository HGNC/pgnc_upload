import unittest
from unittest.mock import MagicMock, patch
from psycopg2.extensions import cursor
from src.pgncdb.inserts import add_gene_location, add_gene_locus_type, add_gene_name, add_gene_symbol

import psycopg2.errors

class TestInserts(unittest.TestCase):

    @patch('src.pgncdb.inserts.warning')
    def test_add_gene_location(self, mock_warning):
        cur = MagicMock(spec=cursor)
        data = [('PotriID1', 'Location1'), ('PotriID2', 'Location2')]

        # Test successful insertion
        cur.rowcount = len(data)
        add_gene_location(cur, data)
        cur.executemany.assert_called_once()
        self.assertFalse(mock_warning.called)

        # Test unique constraint violation
        cur.executemany.side_effect = psycopg2.errors.lookup('23505')
        with self.assertRaises(ValueError):
            add_gene_location(cur, data)

        # Test unsuccessful insertion
        cur.executemany.side_effect = None
        cur.rowcount = 1
        cur.fetchall.return_value = [(1, 1)]
        add_gene_location(cur, data)
        self.assertTrue(mock_warning.called)

    @patch('src.pgncdb.inserts.warning')
    def test_add_gene_locus_type(self, mock_warning):
        cur = MagicMock(spec=cursor)
        data = [('PotriID1', 'LocusType1'), ('PotriID2', 'LocusType2')]

        # Test successful insertion
        cur.rowcount = len(data)
        add_gene_locus_type(cur, data)
        cur.executemany.assert_called_once()
        self.assertFalse(mock_warning.called)

        # Test unique constraint violation
        cur.executemany.side_effect = psycopg2.errors.lookup('23505')
        with self.assertRaises(ValueError):
            add_gene_locus_type(cur, data)

        # Test unsuccessful insertion
        cur.executemany.side_effect = None
        cur.rowcount = 1
        cur.fetchall.return_value = [(1, 1)]
        add_gene_locus_type(cur, data)
        self.assertTrue(mock_warning.called)

    @patch('src.pgncdb.inserts.warning')
    def test_add_gene_name(self, mock_warning):
        cur = MagicMock(spec=cursor)
        data = [('Potri.001G118400', 'cytochrome P450 family 706 subfamily B member 3', 'approved')]

        # Test successful insertion
        cur.rowcount = 1
        cur.fetchone.side_effect = [(1,)]
        add_gene_name(cur, data)
        self.assertEqual(cur.execute.call_count, 2)
        self.assertFalse(mock_warning.called)

        # Test unique constraint violation
        cur.execute.side_effect = psycopg2.errors.lookup('23505')
        with self.assertRaises(ValueError):
            add_gene_name(cur, data)

        # Reset side effects for the next test
        cur.execute.side_effect = None

        # Test unsuccessful insertion
        cur.rowcount = 0
        cur.fetchone.side_effect = [(1,),(1,1)]  # Ensure enough values for fetchone
        add_gene_name(cur, data)
        self.assertTrue(mock_warning.called)

    @patch('src.pgncdb.inserts.warning')
    def test_add_gene_symbol(self, mock_warning):
        cur = MagicMock(spec=cursor)
        data = [('Potri.001G118400', 'AB1', 'approved')]

        # Test successful insertion
        cur.rowcount = 1
        cur.fetchone.side_effect = [(1,)]
        add_gene_symbol(cur, data)
        self.assertEqual(cur.execute.call_count, 2)
        self.assertFalse(mock_warning.called)

        # Test unique constraint violation
        cur.execute.side_effect = psycopg2.errors.lookup('23505')
        with self.assertRaises(ValueError):
            add_gene_symbol(cur, data)

        # Reset side effects for the next test
        cur.execute.side_effect = None

        # Test unsuccessful insertion
        cur.rowcount = 0
        cur.fetchone.side_effect = [(1,),(1,1)]  # Ensure enough values for fetchone
        add_gene_symbol(cur, data)
        self.assertTrue(mock_warning.called)

if __name__ == '__main__':
    unittest.main()