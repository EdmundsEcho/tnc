```rust

pub struct PartitionStrategy {
/// Which canonical columns define partition boundaries
/// e.g., [("parish", Monthly), ("month", Monthly)]
pub partition_keys: Vec<PartitionKey>,
}

impl PartitionStrategy {
/// Decompose a user request into partition coordinates + remainder filter
pub fn decompose(&self, request: &UserRequest)
-> (Vec<PartitionCoord>, SubsetRequest)
{
// Filters on partition columns → PartitionCoords (cache keys)
// Everything else → SubsetRequest
}
}

```

This doesn't belong in `synapse-etl-unit` — it belongs in the resolution layer, because it's about _how data is stored and fetched_, not about _how data is transformed_. The ETL library doesn't care whether a DataFrame came from one partition or five stitched together.

## The flow with LRU

Here's how the pieces fit together:

```

User Request
│
▼
PartitionStrategy.decompose()
│
├── PartitionCoords: [(Acadia, 2025-01)]
│ │
│ ▼
│ LRU.get_or_hydrate()
│ │
│ ├── cache hit → DataFrame
│ │
│ └── cache miss
│ │
│ ├── S3 source: fetch partition parquet
│ ├── API source: fetch recent gap (if needed)
│ │
│ ▼
│ Merge sources (priority-based dedup)
│ │
│ ▼
│ Universe (cached in LRU)
│
└── SubsetRequest: {stations: [A,B,C], day: 15}
│
▼
Universe.filter(SubsetRequest)
│
▼
Response DataFrame
```
