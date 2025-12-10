# Recommended Search Algorithm Cutoffs

Based on minimum batch time (8192 lookups per batch).

- **Int8**: branchless (1-16), hashset (17-64), branchless (65-256)
- **Int16**: use branchless up to 16, then hashset
- **Int32**: always use branchless
