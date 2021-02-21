import pytest
from file_op import extract_data
from os.path import join
from tempfile import NamedTemporaryFile, TemporaryDirectory
from tests.mocks import get_mock_json

pytestmark = pytest.mark.unittests

#########################
### extract_data tests
##########
def test_extract_data():
    """This tests the extraction function from a JSON file. NamedTemporaryFile/Temporary files are used
    for creating temporary environments to store the outputs/inputs and read from them for assertions.
    """
    expected = [
        "id,event_type,event_ts\n",
        "foo,created,2020-12-08 20:03:16.759617\n",
        "bar,created,2014-12-08 20:03:16.759617\n",
    ]
    with NamedTemporaryFile() as inputfile:
        inputfile.write(get_mock_json().encode("utf-8"))
        inputfile.flush()
        with TemporaryDirectory(dir="/tmp") as tmpdir:
            outputfile = join(tmpdir, "test")
            extract_data(src_path=inputfile.name, dst_path=outputfile)
            with open(outputfile, "r") as f:
                data = f.readlines()
                assert expected == data


def test_extract_data_filter():
    """This tests the extraction function from a JSON file. NamedTemporaryFile/Temporary files are used
    for creating temporary environments to store the outputs/inputs and read from them for assertions.
    In here, the filtering mechanism is tested where a specific date is passed.
    """
    expected = ["id,event_type,event_ts\n", "foo,created,2020-12-08 20:03:16.759617\n"]
    with NamedTemporaryFile() as inputfile:
        inputfile.write(get_mock_json().encode("utf-8"))
        inputfile.flush()
        with TemporaryDirectory(dir="/tmp") as tmpdir:
            outputfile = join(tmpdir, "test")
            extract_data(
                src_path=inputfile.name,
                dst_path=outputfile,
                date_filter_key="event_ts",
                date_filter_val="2020-12-08",
            )
            with open(outputfile, "r") as f:
                data = f.readlines()
                assert expected == data
