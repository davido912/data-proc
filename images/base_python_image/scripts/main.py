import sys
from file_op import import_source

from pathlib import Path
from os.path import join


ROOT_DIR = Path(__file__).parent.absolute()
TABLE_METADATA_DIR = join(ROOT_DIR, "table_metadata")


def main(date_filter_val=None):
    import_source(tables_md_dir=TABLE_METADATA_DIR, date_filter_val=date_filter_val)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        main(date_filter_val=sys.argv[1])
    elif len(sys.argv) == 1:
        main()
    else:
        raise ValueError
