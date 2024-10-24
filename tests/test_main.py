import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from src.main import get_file_data, process_df

class TestMainFunctions(unittest.TestCase):

    @patch('src.main.pd.read_csv')
    def test_get_file_data_all_columns(self, mock_read_csv):
        # Mock the return value of pd.read_csv
        mock_df = pd.DataFrame({
            'Gene symbol': ['symbol1', 'symbol2'],
            'Symbol type': ['type1', 'type2'],
            'Gene name': ['name1', 'name2'],
            'Name type': ['type1', 'type2'],
            'Location': ['loc1', 'loc2'],
            'Locus type': ['type1', 'type2']
        })
        mock_read_csv.return_value = mock_df

        file_path = 'dummy_path.tsv'
        result = get_file_data(file_path)

        mock_read_csv.assert_called_once_with(file_path, sep='\t', index_col='PotriID')
        pd.testing.assert_frame_equal(result, mock_df)

    @patch('src.main.pd.read_csv')
    def test_get_file_data_specific_columns(self, mock_read_csv):
        # Mock the return value of pd.read_csv
        mock_df = pd.DataFrame({
            'Gene symbol': ['symbol1', 'symbol2'],
            'Symbol type': ['type1', 'type2']
        })
        mock_read_csv.return_value = mock_df

        file_path = 'dummy_path.tsv'
        cols = ['Gene symbol', 'Symbol type']
        result = get_file_data(file_path, cols)

        mock_read_csv.assert_called_once_with(file_path, sep='\t', index_col='PotriID', usecols=cols)
        pd.testing.assert_frame_equal(result, mock_df)

    def test_get_file_data_invalid_columns(self):
        file_path = 'dummy_path.tsv'
        cols = ['Invalid column']

        with self.assertRaises(ValueError) as context:
            get_file_data(file_path, cols)
        self.assertTrue(
            str(context.exception).startswith(
            "Invalid columns found. Allowed columns are:"
            )
        )
        for column in [
            'Gene symbol', 'Symbol type', 'Gene name', 'Name type', 'Location', 'Locus type'
        ]:
            self.assertIn(column, str(context.exception))

    @patch('src.main.add_gene_location')
    @patch('src.main.add_gene_locus_type')
    @patch('src.main.add_gene_name')
    @patch('src.main.add_gene_symbol')
    def test_process_df(
        self, mock_add_gene_symbol, mock_add_gene_name,
        mock_add_gene_locus_type, mock_add_gene_location
    ):
        df = pd.DataFrame({
            'Location': ['loc1', 'loc2'],
            'Locus type': ['type1', 'type2'],
            'Gene symbol': ['symbol1', 'symbol2'],
            'Symbol type': ['type1', 'type2'],
            'Gene name': ['name1', 'name2'],
            'Name type': ['type1', 'type2']
        }, index=['PotriID1', 'PotriID2'])

        mock_cursor = MagicMock()

        process_df(df, mock_cursor)

        mock_add_gene_location.assert_called_once_with(mock_cursor, [('PotriID1', 'loc1'), ('PotriID2', 'loc2')])
        mock_add_gene_locus_type.assert_called_once_with(mock_cursor, [('PotriID1', 'type1'), ('PotriID2', 'type2')])
        mock_add_gene_name.assert_called_once_with(mock_cursor, [('PotriID1', 'name1', 'type1'), ('PotriID2', 'name2', 'type2')])
        mock_add_gene_symbol.assert_called_once_with(mock_cursor, [('PotriID1', 'symbol1', 'type1'), ('PotriID2', 'symbol2', 'type2')])

if __name__ == '__main__':
    unittest.main()