# graphql-windowed-flux-batch
This repository was created in order to help with [a question](https://spectrum.chat/graphql-java/general/windowing-a-flux-for-batching~c954d2df-7099-4df3-89b0-6f3d94820043) I had on windowing a batch in graphql-java v16. I wanted to process a stream which needed an enrichment call. I wanted to batch some of the enrichments together, but without loading the entire initial set of data into memory.

I was never able to achieve my goal using graphql-java, but I did succeed with scala and Sangria [see here](https://github.com/FilKarnicki/graphql-sangria-windowed-stream-batch)
