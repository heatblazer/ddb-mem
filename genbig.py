import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

#rows_per_batch = 100_000
#total_rows = 1_000_000
rows_per_batch = 5_000_000
total_rows = 100_000_000

output = "heavy_expand.parquet"

writer = None

for i in range(0, total_rows, rows_per_batch):
    print(f"Writing rows {i} to {i + rows_per_batch}")
    df = pd.DataFrame({
        "id": np.arange(i, i + rows_per_batch),
        "constant_string": ["x" * 500] * rows_per_batch,
        "zeros": [0] * rows_per_batch,
        "ints": np.random.randint(0, 1000, size=rows_per_batch),
        "floats": np.random.normal(0, 1, size=rows_per_batch)
    })

    table = pa.Table.from_pandas(df)

    if writer is None:
        writer = pq.ParquetWriter(output, table.schema, compression="zstd", use_dictionary=True)

    writer.write_table(table)
    del df, table  # Free memory

if writer:
    writer.close()

print("✅ Done writing parquet file.")
