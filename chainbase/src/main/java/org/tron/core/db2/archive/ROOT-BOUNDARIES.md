# Retained root-package boundaries

This is the completed source-boundary disposition for the directory reorganization. It is not a
claim that every storage implementation is independently packaged or that persistent formats changed.
Further splitting of these private groups is optional future refactoring, not required directory work.

The Archive-owned root package is already isolated from upstream host sources. Its 99 Java files
contain 43 non-public top-level types. Moving them mechanically would expand private APIs or mix
lifetime/recovery changes with a directory refactor. Existing Gradle modules stay unchanged.

## Authority, recovery and shared contracts (35)

Progress files, participant descriptors, mutation intents and recovery stages share package-private authorities. Keep their codecs and owners together; the shared block/value contracts remain stable host entry points.

- [ArchiveAuthorityHandleSources.java](ArchiveAuthorityHandleSources.java)
- [ArchiveAuthoritySnapshotCollector.java](ArchiveAuthoritySnapshotCollector.java)
- [ArchiveAuthoritySourceBundle.java](ArchiveAuthoritySourceBundle.java)
- [ArchiveBaseManifest.java](ArchiveBaseManifest.java)
- [ArchiveBootstrapAnchor.java](ArchiveBootstrapAnchor.java)
- [ArchiveDirectories.java](ArchiveDirectories.java)
- [ArchiveFormatAdmissionValidator.java](ArchiveFormatAdmissionValidator.java)
- [ArchiveGenerationCapsule.java](ArchiveGenerationCapsule.java)
- [ArchiveParticipantDescriptor.java](ArchiveParticipantDescriptor.java)
- [ArchiveParticipantMutation.java](ArchiveParticipantMutation.java)
- [ArchiveParticipantProgressSource.java](ArchiveParticipantProgressSource.java)
- [ArchivePersistenceException.java](ArchivePersistenceException.java)
- [ArchiveProgressEnvelope.java](ArchiveProgressEnvelope.java)
- [ArchiveProgressEnvelopeCodec.java](ArchiveProgressEnvelopeCodec.java)
- [ArchiveProgressFile.java](ArchiveProgressFile.java)
- [ArchiveRestartCheckpoint.java](ArchiveRestartCheckpoint.java)
- [ArchiveStateBarrier.java](ArchiveStateBarrier.java)
- [ArchiveStoreScope.java](ArchiveStoreScope.java)
- [ArchiveTargetMutationPlan.java](ArchiveTargetMutationPlan.java)
- [ArchiveTargetMutationPlanCodec.java](ArchiveTargetMutationPlanCodec.java)
- [ArchiveTargetMutationPlanFile.java](ArchiveTargetMutationPlanFile.java)
- [ArchiveTruncationIntent.java](ArchiveTruncationIntent.java)
- [ArchiveTruncationRecovery.java](ArchiveTruncationRecovery.java)
- [ArchiveWalBinding.java](ArchiveWalBinding.java)
- [ArchiveWalBindingCodec.java](ArchiveWalBindingCodec.java)
- [ArchiveWalStartupValidator.java](ArchiveWalStartupValidator.java)
- [BlockChangeView.java](BlockChangeView.java)
- [BlockReverseDiff.java](BlockReverseDiff.java)
- [CheckpointPointHistory.java](CheckpointPointHistory.java)
- [DurableHistoryMarkerRangeEvidence.java](DurableHistoryMarkerRangeEvidence.java)
- [LatestStateGenerationCoordinatorFactory.java](LatestStateGenerationCoordinatorFactory.java)
- [OldValue.java](OldValue.java)
- [SharedCheckpointDatabase.java](SharedCheckpointDatabase.java)
- [StateArchiveCanonicalBlockMetaSource.java](StateArchiveCanonicalBlockMetaSource.java)
- [StateArchiveRuntimeOwner.java](StateArchiveRuntimeOwner.java)

## Physical history Stores and recovery (12)

Store handles, scan anchors, truncation and durable publication share package-private constructors and sequencing. The migrated readers use ArchiveHistoryReadHandles as their narrow bridge.

- [ArchiveHistoryReadHandles.java](ArchiveHistoryReadHandles.java)
- [ArchiveHistoryScanAnchor.java](ArchiveHistoryScanAnchor.java)
- [ArchiveHistoryTruncator.java](ArchiveHistoryTruncator.java)
- [ArchiveHistoryWriter.java](ArchiveHistoryWriter.java)
- [HistoryBodyStore.java](HistoryBodyStore.java)
- [HistoryCommitMarkerCodec.java](HistoryCommitMarkerCodec.java)
- [HistoryCommitStore.java](HistoryCommitStore.java)
- [HistoryIndexStore.java](HistoryIndexStore.java)
- [HistorySegmentStore.java](HistorySegmentStore.java)
- [HistorySuffixRecovery.java](HistorySuffixRecovery.java)
- [HistoryTailStore.java](HistoryTailStore.java)
- [PartitionedHistoryBodyStore.java](PartitionedHistoryBodyStore.java)

## Physical V5 history lifetime (14)

Five-lane writers, committed views, data-handle pools, index lanes and reader generations share lifetime and recovery hooks. Preserve the existing private group rather than exposing it to obtain a new package.

- [StateArchiveBlockFrameCodecV5.java](StateArchiveBlockFrameCodecV5.java)
- [StateArchiveCommittedViewV5.java](StateArchiveCommittedViewV5.java)
- [StateArchiveDataHandlePoolV5.java](StateArchiveDataHandlePoolV5.java)
- [StateArchiveFileFormatV3.java](StateArchiveFileFormatV3.java)
- [StateArchiveFiveLaneWriterV5.java](StateArchiveFiveLaneWriterV5.java)
- [StateArchiveGethFormatV5.java](StateArchiveGethFormatV5.java)
- [StateArchiveLaneIndexV5.java](StateArchiveLaneIndexV5.java)
- [StateArchivePointReaderV5.java](StateArchivePointReaderV5.java)
- [StateArchivePointSource.java](StateArchivePointSource.java)
- [StateArchiveProcessFdAdmissionV5.java](StateArchiveProcessFdAdmissionV5.java)
- [StateArchiveReaderGenerationHolderV5.java](StateArchiveReaderGenerationHolderV5.java)
- [StateArchiveReaderGenerationV5.java](StateArchiveReaderGenerationV5.java)
- [StateArchiveSegmentHeaderV5.java](StateArchiveSegmentHeaderV5.java)
- [StateArchiveTailV5.java](StateArchiveTailV5.java)

## Typed queries and physical capture (8)

AccountAsset resolvers share P66 keys and package-private helpers with collection; HistoricalForkView shares session terminal state. Independent query controls and snapshots already live in query subpackages.

- [ArchiveReadContext.java](ArchiveReadContext.java)
- [HistoricalAccountAssetBalanceResolver.java](HistoricalAccountAssetBalanceResolver.java)
- [HistoricalAccountAssetPrefixResolver.java](HistoricalAccountAssetPrefixResolver.java)
- [HistoricalForkView.java](HistoricalForkView.java)
- [HistoricalQuerySession.java](HistoricalQuerySession.java)
- [P66AccountAssetCodec.java](P66AccountAssetCodec.java)
- [PhysicalSnapshotPathStateCollector.java](PhysicalSnapshotPathStateCollector.java)
- [SnapshotOldValueCollector.java](SnapshotOldValueCollector.java)

## Checkpoint and runtime owners (12)

Common checkpoint preparation and physical publication depend on the same private Store/authority group. The public planner and publisher contracts already live in checkpoint.

- [ArchiveRuntimeAttachment.java](ArchiveRuntimeAttachment.java)
- [StateArchiveAppendCheckpointMaterializerV5.java](StateArchiveAppendCheckpointMaterializerV5.java)
- [StateArchiveAppendFileRuntime.java](StateArchiveAppendFileRuntime.java)
- [StateArchiveAppendReadAdapterV3.java](StateArchiveAppendReadAdapterV3.java)
- [StateArchiveCheckpointMaterializer.java](StateArchiveCheckpointMaterializer.java)
- [StateArchiveCheckpointReadAdapter.java](StateArchiveCheckpointReadAdapter.java)
- [StateArchiveCheckpointReadSnapshot.java](StateArchiveCheckpointReadSnapshot.java)
- [StateArchiveCheckpointServingIndex.java](StateArchiveCheckpointServingIndex.java)
- [StateArchiveHotBatchDescriptor.java](StateArchiveHotBatchDescriptor.java)
- [StateArchiveHotBatchDescriptorCodec.java](StateArchiveHotBatchDescriptorCodec.java)
- [StateArchiveHotCheckpointMaterializer.java](StateArchiveHotCheckpointMaterializer.java)
- [StateArchiveHotStore.java](StateArchiveHotStore.java)

## Serving implementation owners (18)

The service, history source, fault categories, index Stores and build coordinator share private constructors, timing and recovery hooks. Backend-neutral models already live in serving; keep implementation ownership cohesive.

- [PersistentServingKeyIndexCatalog.java](PersistentServingKeyIndexCatalog.java)
- [PersistentServingKeyIndexGeneration.java](PersistentServingKeyIndexGeneration.java)
- [ServingBatchPlanner.java](ServingBatchPlanner.java)
- [ServingFailure.java](ServingFailure.java)
- [ServingHistorySource.java](ServingHistorySource.java)
- [ServingIndexIncrementalPlan.java](ServingIndexIncrementalPlan.java)
- [ServingIndexService.java](ServingIndexService.java)
- [ServingIndexStore.java](ServingIndexStore.java)
- [ServingIndexTiming.java](ServingIndexTiming.java)
- [ServingLease.java](ServingLease.java)
- [ServingState.java](ServingState.java)
- [ServingView.java](ServingView.java)
- [StateArchiveIndexDatabase.java](StateArchiveIndexDatabase.java)
- [StateArchiveIndexEngineManifest.java](StateArchiveIndexEngineManifest.java)
- [StateArchiveServingIndexBuildCoordinatorV3.java](StateArchiveServingIndexBuildCoordinatorV3.java)
- [StateArchiveServingSource.java](StateArchiveServingSource.java)
- [StateArchiveServingSourceV5.java](StateArchiveServingSourceV5.java)
- [StateArchiveServingWorkerV3.java](StateArchiveServingWorkerV3.java)
