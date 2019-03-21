# CloudComparison

Efficiently determining large file differences at scale

Data ingestion: Spark for batch processing
Data storage: Redis or Redshift for temporary storage
Processing: Implement algorithms in Spark
Frontend: Dash, flask, command line. Dash allows for direct file uploads (up to ~1GB, then it fails)

Tasks:
* Compress feature space through hash algorithm
* Implement difference algorithms in Spark
* Parallelize job across multiple nodes

Stretch goal: Autoscale based on jobs

Business value: By comparing code against some existing repository, companies can make sure projects aren't copying code.
