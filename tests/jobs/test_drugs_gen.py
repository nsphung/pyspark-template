import filecmp
import os

from click.testing import CliRunner

from pyspark_template.jobs.drugs_gen import drugs_gen


def test_greet_cli() -> None:
    # Given
    output_file: str = "data/output/test_result.json"
    expected_file: str = "data/output/result.json"
    runner = CliRunner()

    # When
    result = runner.invoke(drugs_gen, ["-o", output_file])

    # Then
    assert result.exit_code == 0
    assert f"Writing drugs result in file {output_file}" in result.output
    assert filecmp.cmp(
        output_file, expected_file
    ), f"Generated '{output_file}' should have the same content as file '{expected_file}'"

    # Clean
    os.remove(output_file)
