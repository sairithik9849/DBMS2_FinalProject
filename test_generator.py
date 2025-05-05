"""
-------------------------------------------------------
test_generator.py - ESQL vs SQL Output Validator
Author: Sairithik Komuravelly (Team: NoJoinZone)
Description:
    This script tests the correctness of the auto-generated MF query program
    (_generated.py) by comparing its output with that of a direct SQL query 
    executed via sql.py.

    Workflow:
    - Runs generator.py to create _generated.py based on input.json
    - Executes both _generated.py and sql.py
    - Captures their outputs, normalizes formatting, and compares results

    If the outputs match:
        ✅ Prints confirmation message
    If the outputs differ:
        ❌ Prints both ESQL and SQL results for debugging

    This ensures the generated ESQL logic faithfully reproduces correct SQL behavior.
-------------------------------------------------------
"""

from generator import main as generator
from _generated import query as _generated
from sql import query as sql

def normalize_output(output: str) -> list[str]:
    """
    Normalize tabulated output for consistent comparison:
    - Strip whitespace
    - Remove header separators (---)
    - Sort rows (excluding header)
    """
    lines = output.strip().splitlines()
    if len(lines) <= 2:
        return lines  # header + no data
    header = lines[0]
    rows = sorted(lines[2:])  # Skip separator
    return [header] + rows

def test_generator():
    print("📦 Running generator to create _generated.py...")

    print("🔍 Fetching output from generated code and SQL...")
    generated_output = _generated()
    sql_output = sql()

    norm_generated = normalize_output(generated_output)
    norm_sql = normalize_output(sql_output)

    assert norm_generated == norm_sql, (
        "\n❌ Mismatch between generated and SQL output:\n\n"
        f"Generated:\n{generated_output}\n\n"
        f"SQL:\n{sql_output}\n"
    )

    print("✅ Test passed: Output from generated code matches SQL query output.")

if __name__ == "__main__":
    test_generator()
