# SQL Grouping Sets in Pandas

A Python implementation of SQL-style `GROUPING SETS` / `CUBE` functionality using pandas.

## Overview

This module provides the `grouper()` function, which produces aggregate DataFrames with subtotals at multiple grouping levels - similar to SQL's `GROUPING SETS`, `ROLLUP`, or `CUBE` operations.

## Features

- Generate all combinations of groupings from specified columns
- Automatic detection of single-value columns to reduce redundant aggregations
- Descriptive labels for aggregated dimensions (e.g., `(All Areas)` instead of generic `(All)`)
- Custom aggregation functions

## Usage

```python
import pandas as pd
from sql_grouping_sets_in_pandas import grouper

# Sample data
df = pd.DataFrame({
    'Area': ['a', 'a', 'b', 'b'],
    'Year': [2014, 2014, 2014, 2015],
    'Month': [1, 2, 3, 4],
    'Total': [5, 6, 7, 8],
})

# Define aggregation function
def agg(grpbyobj):
    tmp = pd.DataFrame()
    tmp['Total (n)'] = grpbyobj['Total'].sum()
    return tmp

# Generate grouping sets
result = grouper(df, grpby=['Area', 'Year'], aggfunc=agg)
print(result)
```

### Output

```
          Area         Year  Total (n)
0  (All Areas)         2014         18
1  (All Areas)         2015          8
2  (All Areas)  (All Years)         26
3            a         2014         11
4            a  (All Years)         11
5            b         2014          7
6            b         2015          8
7            b  (All Years)         15
```

## How It Works

1. **Powerset Generation**: Creates all possible combinations of grouping columns
2. **Optimization**: Groups equivalent aggregations together (columns with single unique values don't affect grouping results)
3. **Aggregation**: Performs each unique aggregation only once, then reuses results
4. **Labeling**: Non-grouped columns are labeled with descriptive placeholders like `(All Areas)`

## Requirements

- Python 3.x
- pandas

## License

MIT
