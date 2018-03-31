# Changelog

v0.1.5 (unreleased)
-------------------

- Nothing changed yet.


v0.1.4 (2018-03-31)
-------------------

- Fix DLQMetadata decoding to use DLQMetadataDecoder func instead of inferred decoding from TopicType.
- Fix consumer to use noopDLQ if RetryQ or DLQ in config is empty.
- Fix ResetOffset fails on partition rebalance.
- Add delay to Topic configuration


v0.1.3 (2018-03-09)
-------------------

- Add WithRetryTopics and WithDLQTopics to inject additional consumers for additional retry or DLQ topics.


v0.1.2 (2018-03-07)
-------------------

- Pin sarama-cluster to 2.1.13.


v0.1.1 (2018-03-05)
-------------------

- Fixed sarama-cluster dependency pin to cf455bc755fe41ac9bb2861e7a961833d9c2ecc3 because we need ResetOffsets method with NPE fix.


v0.1.0 (2018-03-05)
-------------------

- Added initial release
