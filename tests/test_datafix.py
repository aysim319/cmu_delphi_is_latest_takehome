import pytest
import pandas as pd
from pathlib import Path

TEST_ROOT = Path(__file__).resolve().parent
class TestDataFix:
    def test_get_latest_entries(self):

