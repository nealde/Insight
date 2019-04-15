# StackD

Efficiently detecting duplicate questions at cloud scale.

Every day, Stack Overflow experiences 6,000 new questions, roughly 20% of which are duplicates.  Currently, these duplicates are manually marked by users who answer questions, which is an extremely tedious process and requires both having a working knowledge of previously answered questions and requires that the other post be linked to.

Experts should be spending their time answering relevant questions and helping the community, not finding the source for duplicate questions.


### Tech Stack:

Datset: Google BigQuery Stack Overflow dataset of > 17M posts in CSVs
Data ingestion: Spark for batch processing and Kafka / Spark Streaming for simulated streaming of questions for real-time processing.    
Data storage: S3 for long-term storage, Redis for preprocessed and intermediate data   
Processing: Implement distributed, pairwise cosine correlations in Spark using Python or Cython, as necessary.
Frontend: Dash, to sample input questions and to return similar results if found.  

![tech_stack](/img/techStack.png)

### MVP:

The minimum viable product pulls in a corpus of cleaned text through a CSV in an s3 bucket and computes the TF-IDF and cosine similarity on a submission from that corpus using the spark library ML.

### Engineering Challenges

During development, the streaming service has undergone several stages of optimization.  The first was properly implementing Redis access - originally, the connection was short-lived and rapidly created:
```Python
def fetch():
    r = redis.Redis()
    r.get()
    return
```
However, these connections were slow and would often result in communication errors.  By using a (Singleton Pattern)[wikipedia.org/singleton_pattern], it was possible to establish each connection only once and enable significantly higher Redis throughput.

```Python
_connection = None
def connection()
    global _connection
    if _connection is None:
      _connection = redis.Redis()
    return _connection
```

Another Redis optimzation was the storing of compressed integers and floats directly in bytestrings stored inside of hash ziplists for reduced storage overhead.  This, rather than the naive implementation of going from sparse vectors serialized to strings, reduced the average size of each stored vector from ~8KB to ~1KB, reducing the overall dataset size from 60GB to 14GB, a cost savings of 75% on AWS.

Once these bottlenecks were alleviated, the code was profiled by timing each piece in a jupyter notebook.  While decompressing these numpy arrays is very fast (~1us), turning those arrays into a sparse vector is very slow (~150us), calculating a dot product between vectors is slow (~50us), and calculating norms is slow (~50us), which makes the cosine similarity metric very slow and prevents the code from being able to saturate the Redis database's 70k Ops/s:
<!-- ![Ops/s](/mvp/) -->

By rewriting this code in Cython to calculate the cosine similarity directly from the decompressed matrices, the total cost of this code dropped from ~250us to ~10us, a significant speed increase that allowed even cutting the computational power in half to continue to saturate the Redis database.

This ushered in another round of optimization, where Redis Pipelining was implemented to raise the throughput from 70k Ops/s to 700k Ops/s through asynchronous sets/gets.  This allowed the Cython code to really shine, outperforming the Sparse Vector code by 85%.



<!-- How do we know if a post is a duplicate? By utilizing the post_link table, if link_type_id = 3, then the post_id is a duplicate.  Of the 17 million posts, 760,000 have been marked as duplicates.  While getting accurate results will be a sub-focus, the main focus will be on efficient, parallelizable implementation, reducing processing time, and scaling for real-time application. -->
