# Recommended Search Algorithm Cutoffs

Based on minimum batch time (8192 lookups per batch).

- **Int8**: use branchless up to 128, then hashset
- **Int16**: use branchless up to 64, then hashset
- **Int32**: use branchless up to 32, then hashset
- **Int64**: use branchless up to 16, then hashset
- **Int128**: use branchless up to 4, then hashset
- **String**: use linear up to 2, then hashset
