# CloudCompare

Efficiently determining text similarity at cloud scale.  

Every day, Stack Overflow experiences 8,000 new questions, some of which are duplicates.  Currently, these duplicates are manually marked by users who answer questions, which is an extremely tedious process and requires both having a working knowledge of previously answered question and requires that the other post be linked to. 


### Tech Stack:

Datset: Google BigQuery Stack Overflow dataset of > 17M posts  
Data ingestion: Spark for batch processing or kafka for simulated streaming of questions for real-time processing.  
Data storage: S3 for long-term storage, Redis for temporary storage  
Processing: Implement distributed, pairwise cosine correlations in Spark using Python or Cython, as necessary.  Possibly compress this cosine correlation dimensionality through a hash function.  
Frontend: Dash or flask  

### MVP:

The minimum viable product would allow for question submission and would compute the cosine correlation on a subset of the data filtered by tags and exchange.

### Scoring

How do we know if a post is a duplicate? By utilizing the post_link table, if link_type_id = 3, then the post_id is a duplicate.  Of the 17 million posts, 760,000 have been marked as duplicates.  While getting accurate results will be a sub-focus, the main focus will be on efficient, parallelizable implementation, reducing processing time, and scaling for real-time application.

### Stretch:

Stretch goals: 
* implement more sophisticated similarity functions from [paper](https://www.site.uottawa.ca/~diana/publications/tkdd.pdf)
* Autoscale based on jobs



