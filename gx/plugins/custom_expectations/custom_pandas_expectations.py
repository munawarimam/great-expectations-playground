from great_expectations.dataset import PandasDataset, MetaPandasDataset

class MyCustomPandasDataset(PandasDataset):

    @MetaPandasDataset.column_pair_map_expectation
    def expect_columns_to_be_different(self, column_A, column_B):
        """
        Custom expectation to check if values in 2 columns are different.
        """
        return column_A != column_B