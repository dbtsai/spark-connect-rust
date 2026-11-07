# Known behavior differences from PySpark Connect

Behaviors of the Rust-backed Python client that differ from PySpark Connect 4.3.0, kept
here so that a version upgrade does not rediscover them. Each entry says whether the
difference is intentional or tracked for a fix.

## `createDataFrame`

- **A pandas DataFrame with a StructType of another width** (intentional, SPARK-60902).
  PySpark's pandas conversion zips the DataFrame's columns with the schema's fields. With
  fewer fields than columns it silently drops the extra columns, and with more fields it
  raises PyArrow's `ArrowInvalid`. This client raises `AXIS_LENGTH_MISMATCH` for both
  rather than drop data.
- **NumPy ndarray input** (tracked by SPARK-60772). Not accepted yet.
- **Nested pandas Decimal inference and failures** (related change: SPARK-60696).
  Inferred nested Decimal fields keep precision 38 and the greatest observed scale;
  PySpark infers the smallest precision that covers the values. Existing container and
  non-Decimal inference rules are unchanged. A nested Decimal that does not fit an explicit
  schema raises `PySparkRuntimeError` rather than PySpark's `PySparkValueError` naming its
  pandas column. Both reject the input before a relation is sent.
- **Multiple invalid pandas columns with an explicit Decimal field** (intentional,
  SPARK-60696). Decimal exactness is checked before the native encoder converts other
  fields. If both an earlier non-Decimal column and a later Decimal column are invalid,
  this client reports the Decimal column first; PySpark reports the first invalid column
  in schema order. Both reject the input before a relation is sent.
- **Decimal context hooks during local-row conversion** (intentional, SPARK-60704).
  Quantum and context objects are shared by Decimal type within one conversion and built
  when its first non-null, non-NaN value is converted. PySpark constructs a context for each field's
  converter before converting values. A custom hook that changes `decimal.DefaultContext`
  or mutates its supplied context during a conversion can observe different timing or
  sharing. Changes between `createDataFrame` calls apply to the next conversion.
- **Decimal class rebinding** (SPARK-60704 followup). Local-row Decimal conversion and
  inference recognize the cached C `_decimal.Decimal` class, falling back to the cached
  public `decimal.Decimal` when the accelerator is unavailable. This can differ from
  PySpark when an application rebinds the public class alias during the process.
