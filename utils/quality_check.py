import json, os
import great_expectations as gx
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)
from gx.plugins.custom_expectations.custom_pandas_expectations import MyCustomPandasDataset

class QualityCheckExpectation:
    def __init__(self, root_directory="./gx"):
        self.root_directory = os.path.abspath(root_directory)

    def do_quality_check(self, df, expectation_suite_name: str):
        data_context_config = DataContextConfig(
            store_backend_defaults=FilesystemStoreBackendDefaults(
                root_directory=self.root_directory
            ),
        )
        context = gx.get_context(project_config=data_context_config)

        suite = context.get_expectation_suite(expectation_suite_name)
        dataset = MyCustomPandasDataset(df, expectation_suite=suite)

        result = dataset.validate()

        return result.to_json_dict()
    
    def generate(self, result_data: json, module: str):
        """
        Summarize result from great expectation, it will capture the columns and values unexpected
        Args:
            result_data (json) : It's the result of data validation 
        """
        message = []

        success_percent = result_data["statistics"]["success_percent"]
        message.append("**Data Quality Failed**\n\n"
                       f"Module: {module} \n\n"
                       f"Overall Success Rate: {success_percent:.2f}%")

        if success_percent == 100:
            message.append("\nValidation successful! All expectations passed. No issues detected.")
            return "\n".join(message), success_percent

        failed_columns = []
        unexpected_values = {}

        for result in result_data["results"]:
            expectation = result["expectation_config"]
            success = result["success"]
            
            if expectation["expectation_type"] == "expect_table_columns_to_match_set":
                if not success:
                    mismatched = result["result"]["details"].get("mismatched", {})
                    unexpected_columns = mismatched.get("unexpected", [])
                    failed_columns.extend(unexpected_columns)
            else:
                if "column_A" in expectation["kwargs"] and "column_B" in expectation["kwargs"]:
                    columns = [expectation["kwargs"]["column_A"], expectation["kwargs"]["column_B"]]
                else:
                    columns = [expectation["kwargs"].get("column")]
                
                if not success:
                    partial_unexpected_list = result["result"].get("partial_unexpected_list", [])
                    if partial_unexpected_list:
                        unexpected_values[tuple(columns)] = list(set(map(str, partial_unexpected_list)))
                    failed_columns.append(tuple(columns))

        failed_columns = sorted([", ".join(columns) if isinstance(columns, tuple) else columns for columns in failed_columns])

        if failed_columns:
            message.append("\nColumns Not Passed:")
            for columns in sorted(failed_columns):
                message.append(f"- {columns}")

        if unexpected_values:
            message.append("\nUnexpected Values:")
            for columns, values in sorted(unexpected_values.items()):
                columns_str = ', '.join(columns)
                message.append(f"- {columns_str}: {', '.join(values)}")

        message.append("\nAction Required: Please review the columns and unexpected values above and decide on the appropriate action.")    
        return "\n".join(message), success_percent