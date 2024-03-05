import pandas

from datamart_materialize.utils import SimpleConverter


def csv_to_parquet(source_filename, dest_fileobj):
    import pandas as pd
    df = pd.read_csv(source_filename)

    for i, chunk in enumerate(
        pandas.read_csv(source_filename, iterator=True, chunksize=1)
    ):
        chunk.to_parquet(
            dest_fileobj,
            index=False
        )


class CSVConverter(SimpleConverter):
    """Adapter pivoting a table.
    """
    def transform(self, source_filename, dest_fileobj):
        csv_to_parquet(
            source_filename,
            dest_fileobj,
        )
