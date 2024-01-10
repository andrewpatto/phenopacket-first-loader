
* Dataset can be constructed in batches over time
* Dataset batches can be spread across multiple locations (in order to support local batches being constructed whilst the rest of the dataset lives/stays in the cloud)
* Batches are immutable once submitted
* Each batch is self-contained, though the dataset may not be
* Datasets groups can span batches (i.e. an individual can have objects appear in two batches)
* Object can be corrected by "later" batches
* Batches must be named in a naming system that retains order (e.g. ISO date)
* Filenames must not carry any meta information other than file type (via suffix)



